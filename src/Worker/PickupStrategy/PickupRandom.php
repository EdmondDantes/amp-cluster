<?php
declare(strict_types=1);

namespace CT\AmpPool\Worker\PickupStrategy;

/**
 * Class PickupRandom
 *
 * @package CT\AmpPool\Worker\PickupStrategy
 */
final class PickupRandom            extends PickupStrategyAbstract
{
    public function pickupWorker(array $possibleGroups = [], array $possibleWorkers = [], array $ignoredWorkers = []): ?int
    {
        $pool                       = iterator_to_array($this->iterate($possibleGroups, $possibleWorkers, $ignoredWorkers));
        
        if($pool === []) {
            return null;
        }
        
        return $pool[array_rand($pool)];
    }
}