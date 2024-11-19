<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\WorkersStorage;

class ApplicationState implements ApplicationStateInterface
{
    private \WeakReference|null $storage = null;
    private bool $isLoaded              = false;
    private bool $isReadOnly            = true;

    protected const int ITEM_COUNT      = 9;

    public static function instanciate(WorkersStorageInterface $workersStorage, int $workersCount, bool $isReadOnly = true): static
    {
        $instance                   = new static($workersCount);
        $instance->storage          = \WeakReference::create($workersStorage);
        $instance->isReadOnly       = $isReadOnly;

        return $instance;
    }

    public function __construct(
        private int $workersCount       = 0,
        private int $pid                = 0,
        private int $startedAt          = 0,
        private int $lastRestartedAt    = 0,
        private int $restartsCount      = 0,
        private int $workersErrors      = 0,
        private int $memoryFree         = 0,
        private int $memoryTotal        = 0,
        private float $loadAverage      = 0.0
    ) {
    }

    public function update(): void
    {
        if ($this->isReadOnly) {
            throw new \RuntimeException('The ApplicationState is read-only');
        }

        $storage                    = $this->getStorage();

        if ($storage === null) {
            return;
        }

        $data                       = \pack(
            'Q*',
            $this->workersCount,
            $this->pid,
            $this->startedAt,
            $this->lastRestartedAt,
            $this->restartsCount,
            $this->workersErrors,
            $this->memoryFree,
            $this->memoryTotal,
            (int) ($this->loadAverage * 1000)
        );

        $storage->updateApplicationState($data);
    }

    protected function load(): void
    {
        if ($this->isLoaded) {
            return;
        }

        $this->isLoaded             = true;

        $storage                    = $this->getStorage();

        if ($storage === null) {
            return;
        }

        $data                       = \unpack('Q*', $storage->readApplicationState());

        if ($data === false) {
            throw new \RuntimeException('Failed to read application state');
        }

        if (\count($data) < static::ITEM_COUNT) {
            throw new \RuntimeException('Invalid application state data');
        }

        $this->workersCount         = $data[1];
        $this->pid                  = $data[2];
        $this->startedAt            = $data[3];
        $this->lastRestartedAt      = $data[4];
        $this->restartsCount        = $data[5];
        $this->workersErrors        = $data[6];
        $this->memoryFree           = $data[7];
        $this->memoryTotal          = $data[8];
        $this->loadAverage          = $data[9] / 1000;
    }

    public function read(): void
    {
        $this->isLoaded             = false;
        $this->load();
    }

    public function getStructureSize(): int
    {
        return 8 * static::ITEM_COUNT;
    }

    public function getWorkersCount(): int
    {
        return $this->workersCount;
    }

    public function getPid(): int
    {
        return $this->pid;
    }

    public function getUptime(): int
    {
        return \time() - $this->startedAt;
    }

    public function getStartedAt(): int
    {
        return $this->startedAt;
    }

    public function getLastRestartedAt(): int
    {
        return $this->lastRestartedAt;
    }

    public function getRestartsCount(): int
    {
        return $this->restartsCount;
    }

    public function getWorkersErrors(): int
    {
        return $this->workersErrors;
    }

    public function getMemoryFree(): int
    {
        return $this->memoryFree;
    }

    public function getMemoryTotal(): int
    {
        return $this->memoryTotal;
    }

    public function getLoadAverage(): float
    {
        return $this->loadAverage;
    }

    public function applyPid(): static
    {
        $this->pid                  = \getmypid();
        return $this;
    }

    public function setStartedAt(int $startedAt): static
    {
        $this->startedAt            = $startedAt;
        return $this;
    }

    public function setLastRestartedAt(int $lastRestartedAt): static
    {
        $this->lastRestartedAt      = $lastRestartedAt;
        return $this;
    }

    public function setRestartsCount(int $restartsCount): static
    {
        $this->restartsCount        = $restartsCount;
        return $this;
    }

    public function setWorkersErrors(int $workersErrors): static
    {
        $this->workersErrors        = $workersErrors;
        return $this;
    }

    public function setMemoryFree(int $memoryFree): static
    {
        $this->memoryFree           = $memoryFree;
        return $this;
    }

    public function setMemoryTotal(int $memoryTotal): static
    {
        $this->memoryTotal          = $memoryTotal;
        return $this;
    }

    public function setLoadAverage(float $loadAverage): static
    {
        $this->loadAverage          = $loadAverage;
        return $this;
    }

    public function toArray(): array
    {
        return [
            'workersCount'          => $this->workersCount,
            'uptime'                => $this->getUptime(),
            'startedAt'             => $this->startedAt,
            'lastRestartedAt'       => $this->lastRestartedAt,
            'restartsCount'         => $this->restartsCount,
            'workersErrors'         => $this->workersErrors,
            'memoryFree'            => $this->memoryFree,
            'memoryTotal'           => $this->memoryTotal,
            'loadAverage'           => $this->loadAverage,
        ];
    }

    private function getStorage(): WorkersStorageInterface|null
    {
        return $this->storage?->get();
    }
}
