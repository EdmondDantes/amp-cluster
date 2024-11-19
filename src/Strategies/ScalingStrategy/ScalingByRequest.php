<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\ScalingStrategy;

use IfCastle\AmpPool\EventWeakHandler;
use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;
use Revolt\EventLoop;

final class ScalingByRequest extends WorkerStrategyAbstract implements ScalingStrategyInterface
{
    private int $lastScalingRequest   = 0;
    private string $decreaseCallbackId = '';

    public function __construct(
        private readonly int $decreaseTimeout = 5 * 60,
        private readonly int $decreaseCheckInterval = 5 * 60,
    ) {
    }

    public function requestScaling(int $fromWorkerId = 0): bool
    {
        $workerGroup                = $this->getWorkerGroup();

        if ($workerGroup === null) {
            return false;
        }

        [$lowestWorkerId, $highestWorkerId] = $this->getMinMaxWorkers($workerGroup->getWorkerGroupId());

        if (($highestWorkerId - $lowestWorkerId) >= $workerGroup->getMaxWorkers()) {
            return false;
        }

        // For watcher process, try to scale immediately
        if ($this->getWorkerPool() !== null) {
            $this->tryToScale();
            return true;
        }

        // Inside worker process, send a message to a watcher process
        $this->getWorker()?->sendMessageToWatcher(new ScalingRequest($workerGroup->getWorkerGroupId()));

        return true;
    }

    public function onStarted(): void
    {
        $workerPool                 = $this->getWorkerPool();

        if ($workerPool !== null) {

            $self                   = \WeakReference::create($this);

            $workerPool->getWorkerEventEmitter()->addWorkerEventListener(new EventWeakHandler(
                $this,
                static function (mixed $event, int $workerId = 0) use ($self) {
                    $self->get()?->handleScalingRequest($event, $workerId);
                }
            ));

            $this->decreaseCallbackId = EventLoop::repeat($this->decreaseCheckInterval, $this->decreaseWorkers(...));
        }
    }

    public function onStopped(): void
    {
        if ($this->decreaseCallbackId !== '') {
            EventLoop::cancel($this->decreaseCallbackId);
        }
    }

    public function __destruct()
    {
        if ($this->decreaseCallbackId !== '') {
            EventLoop::cancel($this->decreaseCallbackId);
        }
    }

    private function getMinMaxWorkers(int $groupId): array
    {
        $minWorkerId                = 0;
        $maxWorkerId                = 0;

        foreach ($this->getWorkersStorage()->foreachWorkers() as $workerState) {
            if ($workerState->getGroupId() === $groupId && $workerState->isShouldBeStarted()) {
                if ($minWorkerId === 0) {
                    $minWorkerId = $workerState->getWorkerId();
                } elseif ($maxWorkerId === 0) {
                    $maxWorkerId = $workerState->getWorkerId();
                } elseif ($maxWorkerId < $workerState->getWorkerId()) {
                    $maxWorkerId = $workerState->getWorkerId();
                }
            }
        }

        return [$minWorkerId, $maxWorkerId];
    }

    private function handleScalingRequest(mixed $message, int $workerId = 0): void
    {
        if ($message instanceof ScalingRequest === false) {
            return;
        }

        if ($this->getWorkerGroup()?->getWorkerGroupId() === $message->toGroupId) {
            $this->tryToScale();
        }
    }

    private function tryToScale(): void
    {
        $workerGroup                = $this->getWorkerGroup();

        if ($workerGroup === null) {
            return;
        }

        $this->lastScalingRequest   = \time();

        $workerPool                 = $this->getWorkerPool();

        if ($workerPool === null) {
            return;
        }

        $workerPool->scaleWorkers($workerGroup->getWorkerGroupId(), 1);
    }

    private function decreaseWorkers(): void
    {
        $workerPool                 = $this->getWorkerPool();

        if ($workerPool === null) {
            return;
        }

        if (($this->lastScalingRequest + $this->decreaseTimeout) >= \time()) {
            return;
        }

        $runningWorkers             = $workerPool->countWorkers($this->getWorkerGroup()?->getWorkerGroupId(), onlyRunning: true);

        if ($runningWorkers <= $this->getWorkerGroup()->getMinWorkers()) {
            return;
        }

        $workerPool->scaleWorkers(
            $this->getWorkerGroup()->getWorkerGroupId(),
            -1
        );
    }
}
