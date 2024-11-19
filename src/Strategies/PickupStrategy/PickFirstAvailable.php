<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\PickupStrategy;

final class PickFirstAvailable extends PickupStrategyAbstract
{
    public function pickupWorker(
        array $possibleGroups = [],
        array $possibleWorkers = [],
        array $ignoredWorkers = [],
        int   $priority = 0,
        int   $weight = 0,
        int   $tryCount = 0
    ): ?int {
        foreach ($this->iterate($possibleGroups, $possibleWorkers, $ignoredWorkers) as $workerState) {
            if ($workerState->isReady()) {
                return $workerState->getWorkerId();
            }
        }

        return null;
    }
}
