<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\WorkerPoolMocks;

use Amp\TimeoutCancellation;
use IfCastle\AmpPool\Exceptions\TerminateWorkerException;
use IfCastle\AmpPool\Worker\WorkerEntryPointInterface;
use IfCastle\AmpPool\Worker\WorkerInterface;

final class RestartEntryPoint implements WorkerEntryPointInterface
{
    public static function getFile(): string
    {
        return \sys_get_temp_dir() . '/worker-pool-restart.text';
    }

    public static function removeFile(): void
    {
        $file                       = self::getFile();

        if (\file_exists($file)) {
            \unlink($file);
        }

        if (\file_exists($file)) {
            throw new \RuntimeException('Could not remove file: ' . $file);
        }
    }

    private WorkerInterface $worker;

    public function initialize(WorkerInterface $worker): void
    {
        $this->worker                = $worker;
    }

    public function run(): void
    {
        if (\is_file(self::getFile()) && (int) \file_get_contents(self::getFile()) >= 1) {
            // second time terminate worker
            throw new TerminateWorkerException;
        }

        $this->worker->awaitTermination(new TimeoutCancellation(5));

        if (\is_file(self::getFile())) {
            $content                = \file_get_contents(self::getFile());
            $content                = (int) $content + 1;
        } else {
            $content                = 1;
        }

        \file_put_contents(self::getFile(), $content);
    }
}
