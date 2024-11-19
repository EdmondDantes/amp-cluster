<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\PickupStrategy;

/**
 * Class PickupRandom.
 *
 * @package IfCastle\AmpPool\Worker\PickupStrategy
 */
final class PickupRandom extends PickupStrategyAbstract
{
    public function pickupWorker(
        array $possibleGroups = [],
        array $possibleWorkers = [],
        array $ignoredWorkers = [],
        int   $priority = 0,
        int   $weight = 0,
        int   $tryCount = 0
    ): ?int {
        $pool                       = \iterator_to_array($this->iterate($possibleGroups, $possibleWorkers, $ignoredWorkers));

        if ($pool === []) {
            return null;
        }

        return $pool[\array_rand($pool)]->getWorkerId();
    }
}
