<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\PickupStrategy;

/**
 * The class implements the strategy of selecting workers in a round-robin manner.
 */
final class PickupRoundRobin extends PickupStrategyAbstract
{
    private array                   $usedWorkers    = [];

    public function pickupWorker(
        array $possibleGroups = [],
        array $possibleWorkers = [],
        array $ignoredWorkers = [],
        int   $priority = 0,
        int   $weight = 0,
        int   $tryCount = 0
    ): ?int {
        $anyFound                   = false;

        // Try to return a worker that has not been used yet
        foreach ($this->iterate($possibleGroups, $possibleWorkers, $ignoredWorkers) as $workerState) {

            $anyFound               = true;

            if (false === \in_array($workerState->getWorkerId(), $this->usedWorkers, true)) {
                $this->usedWorkers[] = $workerState->getWorkerId();
                return $workerState->getWorkerId();
            }
        }

        if (false === $anyFound) {
            return null;
        }

        $this->usedWorkers          = [];

        // Returns first available worker
        foreach ($this->iterate($possibleGroups, $possibleWorkers, $ignoredWorkers) as $workerState) {
            $this->usedWorkers[]     = $workerState->getWorkerId();
            return $workerState->getWorkerId();
        }

        return null;
    }
}
