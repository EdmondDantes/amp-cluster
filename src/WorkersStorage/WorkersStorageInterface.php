<?php
declare(strict_types=1);

namespace CT\AmpPool\WorkersStorage;

interface WorkersStorageInterface
{
    public function getWorkerState(int $workerId): WorkerStateInterface;
    
    public function readWorkerState(int $workerId, int $offset = 0): string;
    
    public function updateWorkerState(int $workerId, string $data, int $offset = 0): void;
}