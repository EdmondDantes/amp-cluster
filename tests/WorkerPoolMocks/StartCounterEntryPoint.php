<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\WorkerPoolMocks;

use Amp\TimeoutCancellation;
use IfCastle\AmpPool\Worker\WorkerEntryPointInterface;
use IfCastle\AmpPool\Worker\WorkerInterface;

/**
 * Use file to count how many times workers were started.
 */
final class StartCounterEntryPoint implements WorkerEntryPointInterface
{
    public static function getFile(): string
    {
        return \sys_get_temp_dir() . '/worker-pool-start-counter.text';
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
        $fh                         = \fopen(self::getFile(), 'c+');

        if (\flock($fh, LOCK_EX)) {
            $content                = \fread($fh, 4);

            if (false === $content || $content === '') {
                $content            = 1;
            } else {
                $content            = (int) $content + 1;
            }

            \fseek($fh, 0);
            \fwrite($fh, (string) $content);
            \flock($fh, LOCK_UN);
            \fclose($fh);
        }

        $this->worker->awaitTermination(new TimeoutCancellation(5));
    }
}
