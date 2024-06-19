<?php
declare(strict_types=1);

namespace CT\AmpPool\Worker\WorkerState;

class WorkerState                   implements WorkerStateInterface
{
    const int SIZE = 4 * 4;
    
    public function __construct(protected bool $isReady, protected int $jobCount, protected int $groupId) {}
    
    public function isReady(): bool
    {
        return $this->isReady;
    }
    
    public function getJobCount(): int
    {
        return $this->jobCount;
    }
    
    public function getGroupId(): int
    {
        return $this->groupId;
    }
    
    public function pack(): string
    {
        return pack('L*', $this->isReady ? 1 : 0, $this->jobCount, $this->groupId);
    }
    
    public static function unpack(string $data): self
    {
        $result = unpack('L*', $data);
        
        if(false === $result || count($result) !== 4) {
            throw new \RuntimeException('Failed to unpack data');
        }
        
        [, $isReady, $jobCount, $groupId] = $result;
        
        return new self((bool) $isReady, $jobCount, $groupId);
    }
}