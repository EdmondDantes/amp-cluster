<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\WorkersStorage;

class MemoryUsage implements MemoryUsageInterface
{
    private \WeakReference|null $storage = null;
    private bool $isLoaded = false;
    private bool $isReadOnly = true;
    private array $stats            = [];

    protected const int ITEM_SIZE   = 8;

    public static function instanciate(WorkersStorageInterface $workersStorage, int $workersCount = 0, bool $isReadOnly = true): static
    {
        $instance                   = new static($workersCount);
        $instance->storage          = \WeakReference::create($workersStorage);
        $instance->isReadOnly       = $isReadOnly;

        return $instance;
    }

    public function __construct(private readonly int $workersCount = 0)
    {
    }

    protected function load(): void
    {
        if ($this->isLoaded) {
            return;
        }

        $storage                    = $this->getStorage();

        if ($storage === null) {
            return;
        }

        $data                       = $storage->readMemoryUsage();

        $data                       = \unpack('Q*', $data);

        if ($data === false) {
            throw new \RuntimeException('Failed to read MemoryUsage data');
        }

        if (\count($data) < $this->workersCount) {
            throw new \RuntimeException('Invalid MemoryUsage data size');
        }

        for ($i = 1; $i <= $this->workersCount; $i++) {
            $this->stats[$i]        = $data[$i];
        }

        $this->isLoaded             = true;
    }

    public function update(): void
    {
        if ($this->isReadOnly) {
            throw new \RuntimeException('MemoryUsage is read-only');
        }

        $storage                    = $this->getStorage();

        if ($storage === null) {
            return;
        }

        $storage->updateMemoryUsage(\pack('Q*', ...$this->stats));
    }

    public function read(): void
    {
        $this->isLoaded             = false;
        $this->load();
    }

    public function getStructureSize(): int
    {
        return $this->workersCount * self::ITEM_SIZE;
    }

    public function getWorkersMemoryUsageStat(): array
    {
        $storage                    = $this->getStorage();

        if ($storage === null) {
            return [];
        }

        $data                       = $storage->readMemoryUsage();

        if ($data === '') {
            return [];
        }

        $result                     = [];
        $data                       = \unpack('Q*', $data);

        if (\count($data) !== $this->workersCount) {
            throw new \RuntimeException('Invalid MemoryUsageStat data size');
        }

        for ($i = 1; $i <= $this->workersCount; $i++) {
            $result[$i]             = $data[$i];
        }

        return $result;
    }

    public function getWorkersMemoryUsage(int $workerId): int
    {
        if (false === $this->isLoaded) {
            $this->load();
        }

        return $this->stats[$workerId] ?? 0;
    }

    public function setStats(array $stats): static
    {
        if ($this->isReadOnly) {
            throw new \RuntimeException('MemoryUsage is read-only');
        }

        $this->stats                = $stats;

        return $this;
    }

    public function getStats(): array
    {
        return $this->stats;
    }

    protected function getStorage(): WorkersStorageInterface|null
    {
        return $this->storage?->get();
    }
}
