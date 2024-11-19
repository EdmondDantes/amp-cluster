<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\JobExecutor;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use IfCastle\AmpPool\JobIpc\IpcServer;
use function Amp\async;

final class JobExecutorAsync extends JobExecutorAbstract
{
    private int $jobCount           = 0;
    private array $jobFutures       = [];

    public function __construct(
        private readonly int        $maxJobCount = 100,
        private readonly int        $maxAwaitAllTimeout = 0
    ) {
    }

    public function __serialize(): array
    {
        return [
            'maxJobCount'           => $this->maxJobCount,
            'maxAwaitAllTimeout'    => $this->maxAwaitAllTimeout
        ];
    }

    public function __unserialize(array $data): void
    {
        $this->maxJobCount          = $data['maxJobCount'] ?? 100;
        $this->maxAwaitAllTimeout   = $data['maxAwaitAllTimeout'] ?? 0;
    }

    protected function initIpcServer(): void
    {
        $this->jobIpc               = new IpcServer(workerId: $this->workerId, logger: $this->logger);
    }

    public function runJob(string $data, ?int $priority = null, ?int $weight = null, ?Cancellation $cancellation = null): Future
    {
        $handler                    = \WeakReference::create($this->handler);

        $future                     = async(static function () use ($data, $handler, $cancellation) {

            $handler                = $handler->get();

            if ($handler instanceof JobHandlerInterface) {
                return $handler->handleJob($data, null, $cancellation);
            }

            return null;
        });

        $this->jobFutures[]         = $future;

        $this->jobCount++;

        $self                       = \WeakReference::create($this);

        $future->finally(static function () use ($self, $future) {

            $self                   = $self->get();

            if ($self === null) {
                return;
            }

            $this->jobCount--;

            $index                  = \array_search($future, $self->jobFutures, true);

            if ($index !== false) {
                unset($self->jobFutures[$index]);
            }
        });

        return $future;
    }

    public function canAcceptMoreJobs(): bool
    {
        return $this->jobCount < $this->maxJobCount;
    }

    public function getJobCount(): int
    {
        return $this->jobCount;
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

        Future\awaitAll($this->jobFutures, $cancellation);
    }

    public function stopAll(?\Throwable $throwable = null): bool
    {
        return false;
    }
}
