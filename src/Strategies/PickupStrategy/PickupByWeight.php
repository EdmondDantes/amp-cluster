<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\PickupStrategy;

/**
 * Selects a worker with the smallest weight.
 * Each JOB has its own weight coefficient (integer number great zero).
 * When a Worker starts performing a job, it increases the total weight parameter.
 * The higher the weight of the worker, the more complex jobs they are handling.
 *
 * This algorithm selects the Worker with the lowest possible weight.
 */
class PickupByWeight extends PickupStrategyAbstract
{
    private int $lastMinimalWeight  = 0;

    public function __construct(private readonly int $ignoreWeightLimit = 0)
    {
    }

    public function pickupWorker(
        array $possibleGroups       = [],
        array $possibleWorkers      = [],
        array $ignoredWorkers       = [],
        int   $priority             = 0,
        int   $weight               = 0,
        int   $tryCount             = 0
    ): ?int {

        $foundWorkerId              = null;
        $minimalWeight              = 0;

        foreach ($this->iterate($possibleGroups, $possibleWorkers, $ignoredWorkers) as $workerState) {

            if ($workerState->isReady() === false) {
                continue;
            }

            // If the worker's weight is greater than the limit and $tryCount === 0, we ignore it.
            if ($this->ignoreWeightLimit > 0 && $workerState->getWeight() > $this->ignoreWeightLimit && $tryCount === 0) {
                continue;
            }

            if ($workerState->getWeight() === 0 || $workerState->getJobProcessing() === 0) {
                return $workerState->getWorkerId();
            }

            if ($workerState->getWeight() < $minimalWeight) {
                $minimalWeight      = $workerState->getWeight();
                $foundWorkerId      = $workerState->getWorkerId();
            }
        }

        $this->lastMinimalWeight    = $foundWorkerId === null ? $minimalWeight : 0;

        return $foundWorkerId;
    }

    public function getLastMinimalWeight(): int
    {
        return $this->lastMinimalWeight;
    }
}
