<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\JobExecutor;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\JobIpc\IpcServerInterface;
use IfCastle\AmpPool\JobIpc\JobRequestInterface;
use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;
use IfCastle\AmpPool\WorkerGroup;
use IfCastle\AmpPool\WorkersStorage\WorkerStateInterface;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use function Amp\delay;

abstract class JobExecutorAbstract extends WorkerStrategyAbstract implements JobExecutorInterface
{
    protected JobHandlerInterface|null $handler         = null;
    protected int                     $workerId         = 0;
    protected LoggerInterface|null    $logger           = null;
    protected WorkerStateInterface|null $workerState    = null;
    protected WorkerGroup|null        $group            = null;
    protected IpcServerInterface|null $jobIpc           = null;

    abstract protected function initIpcServer(): void;

    public function defineJobHandler(JobHandlerInterface $handler): void
    {
        if ($this->handler !== null) {
            throw new FatalWorkerException('Job handler is already defined');
        }

        $this->handler              = $handler;
    }

    public function onStarted(): void
    {
        $worker                     = $this->getSelfWorker();

        if (null === $worker) {
            return;
        }

        if ($this->handler === null) {
            throw new FatalWorkerException('Job handler is not defined before starting the Worker. '
                                           .'Please use $worker->group->getJobExecutor()->defineJobHandler() method before starting the Worker.');
        }

        $jobLoopCancellation        = new DeferredCancellation();
        $workerState                = $worker->getWorkerState();

        $worker->defineSoftShutdownHandler(static function () use ($jobLoopCancellation, $worker, $workerState) {
            if (false === $jobLoopCancellation->isCancelled()) {
                $jobLoopCancellation->cancel();
            }

            $workerState->markAsUnReady()->updateStateSegment();
            $worker->stop();
        });

        $this->workerState          = $worker->getWorkerState();
        $this->group                = $worker->getWorkerGroup();
        $this->logger               = $worker->getLogger();
        $this->workerId             = $worker->getWorkerId();

        $this->initIpcServer();

        if ($this->jobIpc === null) {
            throw new FatalWorkerException('IPC Server is not initialized');
        }

        $abortCancellation           = $worker->getAbortCancellation();

        EventLoop::queue($this->jobIpc->receiveLoop(...), $abortCancellation);

        EventLoop::queue(
            $this->jobLoop(...),
            new CompositeCancellation($abortCancellation, $jobLoopCancellation->getCancellation()),
            $abortCancellation
        );
    }

    /**
     * Fiber loop for processing the request queue to create Jobs.
     *
     *
     */
    protected function jobLoop(?Cancellation $cancellation = null, ?Cancellation $abortCancellation = null): void
    {
        $this->workerState->markAsReady()->updateStateSegment();

        try {

            $jobQueueIterator       = $this->jobIpc->getJobQueue()->iterate();
            $selfRef                = \WeakReference::create($this);

            while ($jobQueueIterator->continue($cancellation)) {

                [$channel, $jobRequest] = $jobQueueIterator->getValue();

                if ($jobRequest === null) {
                    continue;
                }

                if (false === $jobRequest instanceof JobRequestInterface) {
                    $this->logger?->error('Invalid job request object', ['jobRequestType' => \get_debug_type($jobRequest)]);
                    continue;
                }

                $future             = $this->runJob(
                    $jobRequest->getData(),
                    $jobRequest->getPriority(),
                    $jobRequest->getWeight(),
                    $cancellation
                );

                EventLoop::queue(static function () use ($channel, $future, $jobRequest, $selfRef, $cancellation) {

                    try {
                        $result     = $future->await($cancellation);
                    } catch (\Throwable $exception) {
                        $selfRef->get()?->workerState->incrementJobErrors();
                        $result     = $exception;
                    }

                    $selfRef->get()?->workerState->jobDequeued($jobRequest->getWeight(), $selfRef->get()->canAcceptMoreJobs());
                    $selfRef->get()?->jobIpc?->sendJobResult($result, $channel, $jobRequest, $cancellation);
                });

                if (false === $this->canAcceptMoreJobs()) {

                    // If the Worker is busy, we will wait for the job to complete
                    $this->workerState->jobEnqueued($jobRequest->getWeight(), false);
                    $this->awaitAll($cancellation);
                } else {
                    /**
                     * Currently, there is already at least one job in the execution queue.
                     * However, since the queue is asynchronous, we are still in the current Fiber.
                     * There may be a situation where the job is written incorrectly
                     * and does not yield control back to our Fiber for a long time.
                     * This will cause the server to think that everything is fine with the Worker
                     * and continue sending other jobs to the queue.
                     *
                     * Therefore, before waiting for the next job,
                     * we deliberately yield control to the EventLoop to allow the already accepted job to start executing.
                     * If the job works correctly and yields control back to the current Fiber, then everything is fine.
                     */

                    try {
                        // Pass control to other workers
                        $this->workerState->jobEnqueued($jobRequest->getWeight(), false);
                        delay(0.0, true, $cancellation);
                    } finally {
                        // If we return here, we are ready to accept new jobs
                        $this->workerState->markAsReady()->updateStateSegment();
                    }
                }
            }
        } catch (CancelledException) {
            // Job loop canceled
        } finally {
            $this->workerState?->markAsUnReady()->updateStateSegment();
            $this->jobIpc?->close();
            $this->jobIpc               = null;

            // Finally, we will wait for all the jobs to complete
            try {
                $this->awaitAll($abortCancellation);
            } catch (CancelledException) {
                // Ignore
            }
        }
    }
}
