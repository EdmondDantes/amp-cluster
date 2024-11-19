<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\JobExecutor;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use IfCastle\AmpPool\Coroutine\Coroutine;
use IfCastle\AmpPool\Coroutine\CoroutineInterface;
use IfCastle\AmpPool\Coroutine\Scheduler;
use IfCastle\AmpPool\Coroutine\SchedulerInterface;
use IfCastle\AmpPool\JobIpc\IpcServer;

/**
 * Class JobRunnerScheduler.
 *
 * Run jobs using a coroutine scheduler.
 *
 * @package IfCastle\AmpPool\Strategies\JobRunner
 */
final class JobExecutorScheduler extends JobExecutorAbstract
{
    private SchedulerInterface  $scheduler;

    public function __construct(
        private int $maxJobCount        = 100,
        private int $defaultPriority    = 10,
        private int $maxAwaitAllTimeout = 0
    ) {
    }

    public function __serialize(): array
    {
        return [
            'maxJobCount'           => $this->maxJobCount,
            'defaultPriority'       => $this->defaultPriority,
            'maxAwaitAllTimeout'    => $this->maxAwaitAllTimeout
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->maxJobCount          = $data['maxJobCount'] ?? 100;
        $this->defaultPriority      = $data['defaultPriority'] ?? 10;
        $this->maxAwaitAllTimeout   = $data['maxAwaitAllTimeout'] ?? 0;
    }

    public function onStarted(): void
    {
        parent::onStarted();

        if ($this->isWorker()) {
            $this->scheduler        = new Scheduler();
        }
    }

    protected function initIpcServer(): void
    {
        $this->jobIpc               = new IpcServer(workerId: $this->workerId, logger: $this->logger);
    }

    public function runJob(string $data, ?int $priority = null, ?int $weight = null, ?Cancellation $cancellation = null): Future
    {
        $handler                    = \WeakReference::create($this->handler);

        return $this->scheduler->run(new Coroutine(static function (CoroutineInterface $coroutine) use ($handler, $data, $cancellation) {

            $handler                = $handler->get();

            if ($handler instanceof JobHandlerInterface) {
                return $handler->handleJob($data, $coroutine, $cancellation);
            }

            return null;

        }, $priority ?? $this->defaultPriority), $cancellation);
    }

    public function canAcceptMoreJobs(): bool
    {
        return $this->scheduler->getCoroutinesCount() < $this->maxJobCount;
    }

    public function getJobCount(): int
    {
        return $this->scheduler->getCoroutinesCount();
    }

    public function awaitAll(?Cancellation $cancellation = null): void
    {
        if ($cancellation === null && $this->maxAwaitAllTimeout > 0) {
            $cancellation           = new TimeoutCancellation(
                $this->maxAwaitAllTimeout,
                'JobRunnerAsync::awaitAll() timed out: '.$this->maxAwaitAllTimeout.'s'
            );
        } elseif ($this->maxAwaitAllTimeout > 0) {
            $cancellation           = new CompositeCancellation($cancellation, new TimeoutCancellation(
                $this->maxAwaitAllTimeout,
                'JobRunnerAsync::awaitAll() timed out: '.$this->maxAwaitAllTimeout.'s'
            ));
        }

        $this->scheduler->awaitAll($cancellation);
    }

    public function stopAll(?\Throwable $throwable = null): bool
    {
        $this->scheduler->stopAll($throwable);
        return true;
    }
}
