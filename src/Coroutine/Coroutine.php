<?php /** @noinspection ALL */
declare(strict_types=1);

namespace IfCastle\AmpPool\Coroutine;

use Amp\DeferredFuture;
use Amp\Future;
use Revolt\EventLoop\Suspension;

final class Coroutine implements CoroutineInterface
{
    private DeferredFuture $future;
    private Suspension|null     $suspension          = null;
    private \WeakReference|null $schedulerSuspension = null;

    public function __construct(
        private \Closure|null $closure,
        private readonly int $priority  = 0,
        private int $startAt            = 0,
        private readonly int $timeLimit = 0
    ) {
        $this->future               = new DeferredFuture;

        if ($this->startAt === 0) {
            $this->startAt          = \time();
        }
    }

    public function execute(): mixed
    {
        if (null === $this->closure) {
            throw new \Error('Coroutine is already executed');
        }

        $closure                    = $this->closure;
        $this->closure              = null;

        return $closure($this);
    }

    public function resolve(mixed $data = null): void
    {
        if (false === $this->future->isComplete()) {
            $this->future->complete($data);
        }
    }

    public function fail(\Throwable $exception): void
    {
        if (false === $this->future->isComplete()) {
            $this->future->error($exception);
        }
    }

    public function getSuspension(): ?Suspension
    {
        return $this->suspension;
    }

    public function defineSuspension(Suspension $suspension): void
    {
        if ($this->suspension !== null) {
            throw new \Error('Suspension is already defined');
        }

        $this->suspension           = $suspension;
    }

    public function defineSchedulerSuspension(Suspension $schedulerSuspension): void
    {
        if ($this->schedulerSuspension !== null) {
            throw new \Error('Scheduler is already defined');
        }

        $this->schedulerSuspension = \WeakReference::create($schedulerSuspension);
    }

    public function getFuture(): Future
    {
        return $this->future->getFuture();
    }

    public function suspend(): void
    {
        $this->schedulerSuspension?->get()?->resume();
        $this->suspension?->suspend();
    }

    public function getPriority(): int
    {
        return $this->priority;
    }

    public function getStartAt(): int
    {
        return $this->startAt;
    }

    public function getTimeLimit(): int
    {
        return $this->timeLimit;
    }
}
