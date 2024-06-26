<?php
declare(strict_types=1);

namespace CT\AmpPool\WorkerPoolMocks;

use CT\AmpPool\Worker\WorkerEntryPointInterface;
use CT\AmpPool\Worker\WorkerInterface;

final class RestartEntryPoint implements WorkerEntryPointInterface
{
    public static function getFile(): string
    {
        return sys_get_temp_dir() . '/worker-pool-restart.text';
    }
    
    public static function removeFile(): void
    {
        $file                       = self::getFile();
        
        if(file_exists($file)) {
            unlink($file);
        }
        
        if(file_exists($file)) {
            throw new \RuntimeException('Could not remove file: ' . $file);
        }
    }
    
    public function initialize(WorkerInterface $worker): void
    {
    }
    
    public function run(): void
    {
        if(is_file(self::getFile())) {
            $content                = file_get_contents(self::getFile());
            $content                = (int) $content + 1;
        } else {
            $content                = 1;
        }
        
        file_put_contents(self::getFile(), $content);
    }
}