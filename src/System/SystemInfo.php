<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\System;

use IfCastle\AmpPool\Internal\Safe;
use const Amp\Process\IS_WINDOWS;

final class SystemInfo
{
    public static function countCpuCores(): int
    {
        static $cores;

        if ($cores !== null) {
            return $cores;
        }

        $os = (\stripos(\PHP_OS, 'WIN') === 0) ? 'win' : \strtolower(\PHP_OS);

        switch ($os) {
            case 'win':
                $cmd = 'wmic cpu get NumberOfCores';
                break;
            case 'linux':
            case 'darwin':
                $cmd = 'getconf _NPROCESSORS_ONLN';
                break;
            case 'netbsd':
            case 'openbsd':
            case 'freebsd':
                $cmd = 'sysctl hw.ncpu | cut -d \':\' -f2';
                break;
            default:
                $cmd = null;
                break;
        }

        /** @psalm-suppress ForbiddenCode */
        $execResult = $cmd ? (string) \shell_exec($cmd) : '1';

        if ($os === 'win') {
            $execResult = \explode('\n', $execResult)[1];
        }

        return (int) \trim($execResult);
    }

    /**
     * Determine if SO_REUSEPORT is supported on the system.
     *
     */
    public static function canReusePort(): bool
    {
        static $canReusePort;

        if ($canReusePort !== null) {
            return $canReusePort;
        }

        $os = (\stripos(\PHP_OS, 'WIN') === 0) ? 'win' : \strtolower(\PHP_OS);

        // Before you add new systems, please check whether they really support the option for load balancing,
        // e.g., macOS only supports it for failover, only the newest process will get connections there.
        switch ($os) {
            case 'win':
                $canReusePort = true;
                break;

            case 'linux':
                // We determine support based on a Kernel version.
                // We don't care about backports, as socket transfer works fine, too.
                /** @psalm-suppress ForbiddenCode */
                $version = (string) \shell_exec('uname -r');
                $version = \trim($version);
                $version = \implode('.', \array_slice(\explode('.', $version), 0, 2));
                $canReusePort = \version_compare($version, '3.9', '>=');
                break;

            default:
                $canReusePort = false;
                break;
        }

        return $canReusePort;
    }

    public static function getProcessMemoryUsage(int $pid): int
    {
        if (IS_WINDOWS) {

            $info                   = \shell_exec('wmic process where processid='.$pid.' get workingsetsize /format:csv');

            if (empty($info)) {
                return 0;
            }

            $info                   = \explode("\n", \trim($info));

            if (empty($info[1])) {
                return 0;
            }

            $info                   = \explode(',', $info[1]);

            return (int) $info[1];
        }

        $info                   = \shell_exec('ps -p '.$pid.' -o rss=');

        if (empty($info)) {
            return 0;
        }

        return (int) $info * 1024;
    }

    public static function systemStat(bool $isRecalculate = false): array
    {
        if (self::$instance === null) {
            self::$instance         = new self();
        }

        if ($isRecalculate) {
            self::$instance->isCalculated = false;
        }

        return [
            'memory_total'          => self::$instance->getTotalMemory(),
            'memory_free'           => self::$instance->getFreeMemory(),
            'load_average'          => self::$instance->getLoadAverage(),
        ];
    }

    private static ?SystemInfo $instance = null;

    private ?int $memoryTotal     = null;
    private ?int $memoryFree      = null;
    private ?int $cpuLoad         = null;
    private ?float $loadAverage   = null;

    protected bool $isCalculated    = false;

    public function getTotalMemory(): int
    {
        if (false === $this->isCalculated) {
            $this->calculate();
        }

        return $this->memoryTotal ?? 0;
    }

    public function getFreeMemory(): int
    {
        if (false === $this->isCalculated) {
            $this->calculate();
        }

        return $this->memoryFree ?? 0;
    }

    public function getLoadAverage(): float
    {
        if (false === $this->isCalculated) {
            $this->calculate();
        }

        return $this->loadAverage ?? $this->cpuLoad ?? 0.0;
    }

    private function calculate(): void
    {
        $this->isCalculated         = true;

        if (PHP_OS_FAMILY === 'Windows') {
            $this->defineWindowsMemoryUsage();
            $this->defineWindowsCPULoad();
        } else {
            $this->defineUnixMemoryUsage();
            $this->defineUnixLoadAverage();
        }
    }

    protected function defineUnixMemoryUsage(): void
    {
        try {

            $stats                  = Safe::execute(fn () => \file_get_contents('/proc/meminfo'));
            $stats                  = \str_replace(["\r\n", "\n\r", "\r"], "\n", $stats);
            $stats                  = \explode("\n", $stats);
            $needKeys               = ['MemTotal', 'MemFree'];

            // Separate values and find the correct lines for total and free mem
            foreach ($stats as $statLine) {

                $statLineData       = \explode(':', \trim($statLine));

                // Total memory
                if (\count($statLineData) === 2 && \in_array(\trim($statLineData[0]), $needKeys)) {

                    $value          = (int) (\explode(' ', \trim($statLineData[1]))[0]) * 1024;

                    if (\trim($statLineData[0]) === 'MemTotal') {
                        $this->memoryTotal = $value;
                    } else {
                        $this->memoryFree  = $value;
                    }

                    if ($this->memoryFree !== null && $this->memoryTotal !== null) {
                        break;
                    }
                }
            }

        } catch (\Throwable) {
            $this->memoryTotal      = 0;
            $this->memoryFree       = 0;
        }
    }

    protected function defineWindowsMemoryUsage(bool $getPercentage = true): void
    {
        // Get total physical memory (this is in bytes)
        $output                 = \shell_exec('wmic ComputerSystem get TotalPhysicalMemory');

        if ($output !== false) {
            $output             = \explode("\n", $output);

            if (!empty($output[1])) {
                $this->memoryTotal = (int) \trim($output[1]);
            }
        }

        // Get free physical memory (this is in kilobytes!)
        $output                 = \shell_exec('wmic OS get FreePhysicalMemory');

        if ($output !== false) {
            $output             = \explode("\n", $output);

            if (!empty($output[1])) {
                $this->memoryFree = (int) \trim($output[1]) * 1024;
            }
        }

        if ($this->memoryTotal === null) {
            $this->memoryTotal  = 0;
        }

        if ($this->memoryFree === null) {
            $this->memoryFree   = 0;
        }
    }

    protected function defineUnixLoadAverage(): void
    {
        $output                     = \shell_exec('uptime');

        if (empty($output)) {
            return;
        }

        if (\preg_match('/load average: (?<load>[\d.]+)/i', $output, $m)) {
            $this->loadAverage      = (float) $m['load'];
        }
    }

    protected function defineWindowsCPULoad(): void
    {
        $output                     = \shell_exec('wmic cpu get loadpercentage /all');

        if (empty($output)) {
            return;
        }

        foreach (\explode("\n", $output) as $line) {
            if (!empty($line) && \preg_match("/^[0-9]+\$/", $line)) {
                $this->cpuLoad      = (int) $line;
                break;
            }
        }
    }
}
