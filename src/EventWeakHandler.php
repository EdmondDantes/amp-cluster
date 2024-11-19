<?php
declare(strict_types=1);

namespace IfCastle\AmpPool;

final class EventWeakHandler
{
    private \Closure|null $closure;
    private \WeakReference|null $eventEmitter;
    private \WeakReference|null $self;

    public function __construct(object $self, \Closure $closure)
    {
        $this->closure              = $closure;
        $this->self                 = \WeakReference::create($self);
    }

    public function defineEventEmitter(WorkerEventEmitterInterface $eventEmitter): void
    {
        $this->eventEmitter         = \WeakReference::create($eventEmitter);
    }

    public function __invoke(mixed $event, int $workerId = 0): void
    {
        $eventEmitter               = $this->eventEmitter?->get();
        $self                       = $this->self?->get();

        if ($eventEmitter === null || $this->closure === null) {
            $this->eventEmitter     = null;
            $this->self             = null;
            $this->closure          = null;
            return;
        }

        if ($self === null) {

            $eventEmitter->removeWorkerEventListener($this->closure);

            $this->self             = null;
            $this->closure          = null;
            $this->eventEmitter     = null;
            return;
        }

        $closure                    = $this->closure;

        $closure($event, $workerId);
    }
}
