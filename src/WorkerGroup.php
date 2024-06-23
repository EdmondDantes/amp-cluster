<?php
declare(strict_types=1);

namespace CT\AmpPool;

use CT\AmpPool\Strategies\JobRunner\JobRunnerInterface;
use CT\AmpPool\Strategies\PickupStrategy\PickupStrategyInterface;
use CT\AmpPool\Strategies\RestartStrategy\RestartStrategyInterface;
use CT\AmpPool\Strategies\RunnerStrategy\RunnerStrategyInterface;
use CT\AmpPool\Strategies\ScalingStrategy\ScalingStrategyInterface;
use CT\AmpPool\Strategies\SocketStrategy\SocketStrategyInterface;
use CT\AmpPool\Strategies\WorkerStrategyInterface;
use Psr\Log\LoggerInterface;

/**
 * Data structure for describing a group of workers.
 */
final class WorkerGroup             implements WorkerGroupInterface
{
    public static function startStrategies(array $groupsScheme): void
    {
        foreach ($groupsScheme as $group) {
            foreach ($group->getWorkerStrategies() as $strategy) {
                if($strategy instanceof WorkerStrategyInterface) {
                    $strategy->onStarted();
                }
            }
        }
    }
    
    public static function stopStrategies(array $groupsScheme, LoggerInterface $logger = null): void
    {
        foreach ($groupsScheme as $group) {
            foreach ($group->getWorkerStrategies() as $strategy) {
                
                if(false === $strategy instanceof WorkerStrategyInterface) {
                    continue;
                }
                
                try {
                    $strategy->onStopped();
                } catch (\Throwable $exception) {
                    $logger?->error('Worker strategy "'.get_class($strategy).'" failed to stop', ['exception' => $exception]);
                }
            }
        }
    }
    
    public function __construct(
        private readonly string         $entryPointClass,
        private readonly WorkerTypeEnum $workerType,
        private readonly int            $minWorkers = 0,
        private int                     $maxWorkers = 0,
        private string                  $groupName = '',
        /**
         * @var int[]
         */
        private readonly array          $jobGroups = [],
        private ?RunnerStrategyInterface $runnerStrategy = null,
        private ?PickupStrategyInterface $pickupStrategy = null,
        private ?RestartStrategyInterface $restartStrategy = null,
        private ?ScalingStrategyInterface $scalingStrategy = null,
        private ?SocketStrategyInterface $socketStrategy = null,
        private ?JobRunnerInterface     $jobRunner = null,
        private int                     $workerGroupId = 0,
    ) {}
    
    public function getEntryPointClass(): string
    {
        return $this->entryPointClass;
    }
    
    public function getWorkerType(): WorkerTypeEnum
    {
        return $this->workerType;
    }
    
    public function getWorkerGroupId(): int
    {
        return $this->workerGroupId;
    }
    
    public function getMinWorkers(): int
    {
        return $this->minWorkers;
    }
    
    public function getMaxWorkers(): int
    {
        return $this->maxWorkers;
    }
    
    public function getGroupName(): string
    {
        return $this->groupName;
    }
    
    public function getJobGroups(): array
    {
        return $this->jobGroups;
    }
    
    public function getRunnerStrategy(): ?RunnerStrategyInterface
    {
        return $this->runnerStrategy;
    }
    
    public function getPickupStrategy(): ?PickupStrategyInterface
    {
        return $this->pickupStrategy;
    }
    
    public function getRestartStrategy(): ?RestartStrategyInterface
    {
        return $this->restartStrategy;
    }
    
    public function getScalingStrategy(): ?ScalingStrategyInterface
    {
        return $this->scalingStrategy;
    }
    
    public function getJobRunner(): ?JobRunnerInterface
    {
        return $this->jobRunner;
    }
    
    public function getSocketStrategy(): ?SocketStrategyInterface
    {
        return $this->socketStrategy;
    }
    
    public function defineGroupName(string $groupName): self
    {
        if($this->groupName !== '') {
            throw new \LogicException('Group name is already defined');
        }
        
        $this->groupName            = $groupName;
        
        return $this;
    }
    
    public function defineWorkerGroupId(int $workerGroupId): self
    {
        if($workerGroupId <= 0) {
            throw new \InvalidArgumentException('Worker group ID must be a positive integer');
        }
        
        if($this->workerGroupId !== 0) {
            throw new \LogicException('Worker group ID is already defined');
        }
        
        $this->workerGroupId        = $workerGroupId;
        
        return $this;
    }
    
    public function defineMaxWorkers(int $maxWorkers): self
    {
        if($maxWorkers <= 0) {
            throw new \InvalidArgumentException('Max workers must be a positive integer');
        }
        
        if($this->maxWorkers !== 0) {
            throw new \LogicException('Max workers is already defined');
        }
        
        $this->maxWorkers           = $maxWorkers;
        
        return $this;
    }
    
    public function defineRunnerStrategy(RunnerStrategyInterface $runnerStrategy): self
    {
        if($this->runnerStrategy !== null) {
            throw new \LogicException('Runner strategy is already defined');
        }
        
        $this->runnerStrategy       = $runnerStrategy;
        
        return $this;
    }
    
    public function definePickupStrategy(PickupStrategyInterface $pickupStrategy): self
    {
        if($this->pickupStrategy !== null) {
            throw new \LogicException('Pickup strategy is already defined');
        }
        
        $this->pickupStrategy       = $pickupStrategy;
        
        return $this;
    }
    
    public function defineRestartStrategy(RestartStrategyInterface $restartStrategy): self
    {
        if($this->restartStrategy !== null) {
            throw new \LogicException('Restart strategy is already defined');
        }
        
        $this->restartStrategy      = $restartStrategy;
        
        return $this;
    }
    
    public function defineScalingStrategy(ScalingStrategyInterface $scalingStrategy): self
    {
        if($this->scalingStrategy !== null) {
            throw new \LogicException('Scaling strategy is already defined');
        }
        
        $this->scalingStrategy      = $scalingStrategy;
        
        return $this;
    }
    
    public function defineJobRunner(JobRunnerInterface $jobRunner): self
    {
        if($this->jobRunner !== null) {
            throw new \LogicException('Job runner is already defined');
        }
        
        $this->jobRunner            = $jobRunner;
        
        return $this;
    }
    
    public function defineSocketStrategy(SocketStrategyInterface $socketStrategy): self
    {
        if($this->socketStrategy !== null) {
            throw new \LogicException('Socket strategy is already defined');
        }
        
        $this->socketStrategy       = $socketStrategy;
        
        return $this;
    }
    
    public function getWorkerStrategies(): array
    {
        $strategyList               = [];
        
        if($this->runnerStrategy !== null) {
            $strategyList[]         = $this->runnerStrategy;
        }
        
        if($this->pickupStrategy !== null) {
            $strategyList[]         = $this->pickupStrategy;
        }
        
        if($this->restartStrategy !== null) {
            $strategyList[]         = $this->restartStrategy;
        }
        
        if($this->scalingStrategy !== null) {
            $strategyList[]         = $this->scalingStrategy;
        }
        
        if($this->socketStrategy !== null) {
            $strategyList[]         = $this->socketStrategy;
        }
        
        return $strategyList;
    }
}