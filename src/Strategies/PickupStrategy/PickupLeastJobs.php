<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\PickupStrategy;

/**
 * The algorithm selects a worker based on the number of tasks assigned to them.
 * One worker can handle multiple tasks.
 * We take into account how many tasks have been assigned to each worker
 * and use the one that is currently handling the minimum number of tasks.
 *
 */
final class PickupLeastJobs extends PickupStrategyAbstract
{
    public function pickupWorker(
        array $possibleGroups = [],
        array $possibleWorkers = [],
        array $ignoredWorkers = [],
        int   $priority = 0,
        int   $weight = 0,
        int   $tryCount = 0
    ): ?int {

        $foundWorkerId              = null;
        $bestJobCount               = 0;

        foreach ($this->iterate($possibleGroups, $possibleWorkers, $ignoredWorkers) as $workerState) {

            if ($workerState->isReady() === false) {
                continue;
            }

            if ($workerState->getJobProcessing() === 0) {
                return $workerState->getWorkerId();
            }

            if ($workerState->getJobProcessing() < $bestJobCount) {
                $bestJobCount       = $workerState->getJobProcessing();
                $foundWorkerId      = $workerState->getWorkerId();
            }
        }

        return $foundWorkerId;
    }
}
