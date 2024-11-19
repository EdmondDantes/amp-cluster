<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\WorkersStorage;

final class WorkersStorageMemory implements WorkersStorageInterface
{
    public static function instanciate(int $workersCount = 0): static
    {
        return new static(WorkerState::class, ApplicationState::class, MemoryUsage::class, $workersCount);
    }

    private bool $isWrite           = false;
    private int $structureSize;
    private int         $totalSize;
    private string $buffer          = '';

    public function __construct(
        private readonly string $storageClass,
        private readonly string $applicationClass,
        private readonly string $memoryUsageClass,
        private readonly int $workersCount = 0,
        private readonly int $workerId = 0
    ) {
        $this->structureSize        = $this->getStructureSize();
        $this->totalSize            = $this->getApplicationState()->getStructureSize() +
                                      $this->getMemoryUsage()->getStructureSize() +
                                      $this->structureSize * $this->workersCount;

        if ($this->workersCount > 0) {
            $this->isWrite          = true;
        }
    }

    private function open(): void
    {
        if ($this->buffer !== '') {
            return;
        }

        $this->buffer               = \str_repeat("\0", $this->totalSize);
    }

    private function getStructureSize(): int
    {
        $class                      = $this->storageClass;

        if (\is_subclass_of($class, WorkerStateInterface::class) === false) {
            throw new \RuntimeException('Invalid storage class provided. Expected ' . WorkerStateInterface::class . ' implementation');
        }

        return \forward_static_call([$class, 'getItemSize']);
    }

    public function getWorkerState(int $workerId): WorkerStateInterface
    {
        return \forward_static_call([$this->storageClass, 'instanciateFromStorage'], $this, $workerId);
    }

    public function reviewWorkerState(int $workerId): WorkerStateInterface
    {
        return \forward_static_call([$this->storageClass, 'unpackItem'], $this->readWorkerState($workerId));
    }

    public function foreachWorkers(): array
    {
        $this->open();

        $workers                    = [];

        for ($i = 0; $i < $this->workersCount; $i++) {
            $workers[]              = $this->readWorkerState($i + 1);
        }

        return $workers;
    }

    public function readWorkerState(int $workerId, int $offset = 0): string
    {
        $this->open();

        if ($workerId < 0) {
            throw new \RuntimeException('Invalid worker id provided');
        }

        $baseOffset                 = $this->getApplicationState()->getStructureSize() + $this->getMemoryUsage()->getStructureSize();

        return \substr($this->buffer, $baseOffset + ($workerId - 1) * $this->structureSize + $offset, $this->structureSize);
    }

    public function updateWorkerState(int $workerId, string $data, int $offset = 0): void
    {
        $this->open();

        if ($workerId < 0) {
            throw new \RuntimeException('Invalid worker id provided');
        }

        if (false === $this->isWrite) {
            throw new \RuntimeException('This instance WorkersStorage is read-only');
        }

        $baseOffset                 = $this->getApplicationState()->getStructureSize() + $this->getMemoryUsage()->getStructureSize();

        $this->buffer               = \substr_replace(
            $this->buffer,
            $data,
            $baseOffset + ($workerId - 1) * $this->structureSize + $offset,
            \strlen($data)
        );
    }

    public function getApplicationState(): ApplicationStateInterface
    {
        return \forward_static_call([$this->applicationClass, 'instanciate'], $this, $this->workersCount, $this->workerId !== 0);
    }

    public function readApplicationState(): string
    {
        $this->open();

        $size                       = $this->getApplicationState()->getStructureSize();

        return \substr($this->buffer, 0, $size);
    }

    public function updateApplicationState(string $data): void
    {
        $this->open();

        if (false === $this->isWrite || $this->workerId !== 0) {
            throw new \RuntimeException('This instance WorkersStorage is read-only');
        }

        $size                       = $this->getApplicationState()->getStructureSize();

        if (\strlen($data) !== $size) {
            throw new \RuntimeException('Invalid application state data size');
        }

        $this->buffer               = \substr_replace($this->buffer, $data, 0, $size);
    }

    public function getMemoryUsage(): MemoryUsageInterface
    {
        return \forward_static_call([$this->memoryUsageClass, 'instanciate'], $this, $this->workersCount, $this->workerId !== 0);
    }

    public function readMemoryUsage(): string
    {
        $this->open();

        $size                       = $this->getMemoryUsage()->getStructureSize();

        return \substr($this->buffer, $this->getApplicationState()->getStructureSize(), $size);
    }

    public function updateMemoryUsage(string $data): void
    {
        $this->open();

        if (false === $this->isWrite || $this->workerId !== 0) {
            throw new \RuntimeException('This instance WorkersStorage is read-only');
        }

        $size                       = $this->getMemoryUsage()->getStructureSize();

        if (\strlen($data) !== $size) {
            throw new \RuntimeException('Invalid memory usage data size');
        }

        $this->buffer               = \substr_replace($this->buffer, $data, $this->getApplicationState()->getStructureSize(), $size);
    }

    public function close(): void
    {
        $this->buffer               = '';
    }
}
