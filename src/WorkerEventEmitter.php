<?php
declare(strict_types=1);

namespace IfCastle\AmpPool;

final class WorkerEventEmitter implements WorkerEventEmitterInterface
{
    private array $listeners        = [];

    public function addWorkerEventListener(callable $listener): void
    {
        if ($listener instanceof EventWeakHandler) {
            $listener->defineEventEmitter($this);
        }

        $this->listeners[]          = $listener;
    }

    public function removeWorkerEventListener(callable $listener): void
    {
        foreach ($this->listeners as $key => $value) {
            if ($value === $listener) {
                unset($this->listeners[$key]);
            }
        }
    }

    public function emitWorkerEvent(mixed $event, int $workerId = 0): void
    {
        foreach ($this->listeners as $key => $listener) {

            if ($listener === null) {
                unset($this->listeners[$key]);
                continue;
            }

            $listener($event, $workerId);
        }
    }

    public function free(): void
    {
        $this->listeners            = [];
    }
}
