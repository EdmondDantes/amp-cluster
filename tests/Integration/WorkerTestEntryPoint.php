<?php
declare(strict_types=1);

namespace CT\AmpServer\Integration;

use Amp\DeferredCancellation;
use CT\AmpServer\Exceptions\FatalWorkerException;
use CT\AmpServer\JobIpc\IpcClient;
use CT\AmpServer\JobIpc\IpcServer;
use CT\AmpServer\Worker\WorkerEntryPointI;
use CT\AmpServer\Worker\WorkerI;
use CT\AmpServer\WorkerTypeEnum;
use Revolt\EventLoop;

final class WorkerTestEntryPoint    implements WorkerEntryPointI
{
    private const string JOB_TEST    = 'JOB_TEST';
    public const string RESULT_FILE  = '/amp_worker_test_result.txt';
    
    private WorkerI $workerStrategy;
    
    #[\Override] public function initialize(WorkerI $workerStrategy): void
    {
        $this->workerStrategy       = $workerStrategy;
    }
    
    #[\Override] public function run(): void
    {
        if($this->workerStrategy->getWorkerType() === WorkerTypeEnum::REACTOR->value) {
            
            $deferredCancellation   = new DeferredCancellation();
            
            $jobIpcClient           = new IpcClient(
                $this->workerStrategy->getWorkerId(),
                $this->workerStrategy->getWorkerGroupId(),
                null,
                $deferredCancellation->getCancellation()
            );
            
            EventLoop::queue($jobIpcClient->mainLoop(...));
            
            EventLoop::delay(5000, static function () use ($deferredCancellation): void {
                $deferredCancellation->cancel(new FatalWorkerException('Timeout for Reactor Worker'));
            });
            
            $resultFuture           = $jobIpcClient->sendJob(
                self::JOB_TEST, $this->workerStrategy->getWorkerGroupId() + 1, true
            );
            
            $result                 = $resultFuture->await($deferredCancellation->getCancellation());
            
            $tmpFile                = sys_get_temp_dir() . self::RESULT_FILE;
            file_put_contents($tmpFile, $result);
            
            $deferredCancellation->cancel(new \Exception('Reactor Worker is done'));
        
        } elseif ($this->workerStrategy->getWorkerType() === WorkerTypeEnum::JOB->value) {
            
            // Code for Job Worker
            $jobIpcServer           = new IpcServer($this->workerStrategy->getWorkerId());
            $deferredCancellation   = new DeferredCancellation();
            
            EventLoop::queue($jobIpcServer->receiveLoop(...), $deferredCancellation->getCancellation());
            
            EventLoop::delay(5000, static function () use ($deferredCancellation): void {
                $deferredCancellation->cancel(new FatalWorkerException('Timeout for Job Worker'));
            });
            
            EventLoop::queue(static function () use ($jobIpcServer, $deferredCancellation): void {
                $iterator           = $jobIpcServer->getJobQueue()->iterate();
                $abortCancellation  = $deferredCancellation->getCancellation();
                
                while($iterator->continue($abortCancellation)) {
                    [$channel, $data]            = $iterator->getValue();
                    
                    if($data === self::JOB_TEST) {
                        $channel->send('OK');
                    } else {
                        $channel->send('ERROR');
                    }
                    
                    $deferredCancellation->cancel(new \Exception('Job Worker is done'));
                }
            });
        }
        
        $this->workerStrategy->awaitTermination();
    }
}