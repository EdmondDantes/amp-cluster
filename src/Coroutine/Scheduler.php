<?php
declare(strict_types=1);

namespace CT\AmpPool\Coroutine;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use CT\AmpPool\Coroutine\Exceptions\CoroutineNotStarted;
use CT\AmpPool\Coroutine\Exceptions\CoroutineTerminationException;
use Revolt\EventLoop;
use Revolt\EventLoop\Suspension;

final class Scheduler               implements SchedulerInterface
{
    /**
     * @var array<CoroutineInterface>
     */
    private array $coroutines       = [];
    private array $coroutinesQueue  = [];
    private int         $highestPriority   = 0;
    private ?Suspension $suspension = null;
    private string      $callbackId = '';
    private bool        $isRunning  = true;
    private ?DeferredFuture $future = null;
    private \Throwable|null $stopException = null;
    private bool $managerResumed    = false;
    
    private function init(): void
    {
        if ($this->callbackId !== '') {
            return;
        }
        
        $this->future               = new DeferredFuture();
        $this->stopException        = null;
        $this->isRunning            = true;
        $this->callbackId    = EventLoop::defer($this->scheduleCoroutines(...));
    }
    
    private function scheduleCoroutines(): void
    {
        $this->suspension           = EventLoop::getSuspension();
        
        while ($this->coroutines !== [] && $this->isRunning) {
            
            $this->managerResumed   = false;
            
            if($this->coroutinesQueue === []) {
                
                $this->highestPriority = 0;
                
                foreach ($this->coroutines as $coroutine) {
                    if($coroutine->getPriority() > $this->highestPriority) {
                        $this->highestPriority = $coroutine->getPriority();
                    }
                }
                
                foreach ($this->coroutines as $coroutine) {
                    if($coroutine->getPriority() === $this->highestPriority) {
                        $this->coroutinesQueue[] = $coroutine;
                    }
                }
            }
            
            $coroutine              = array_shift($this->coroutinesQueue);
            $coroutine->getSuspension()?->resume();
            $this->suspension->suspend();
        }
        
        try {
            
            if($this->stopException !== null) {
                foreach ($this->coroutines as $callbackId => $coroutine) {
                    
                    if($coroutine->getSuspension() === null) {
                        EventLoop::cancel($callbackId);
                    } else {
                        $coroutine->getSuspension()->throw($this->stopException);
                    }
                }
            }
            
        } finally {
            
            $future                     = $this->future;
            $stopException              = $this->stopException;
            
            $this->future               = null;
            $this->coroutinesQueue      = [];
            $this->coroutines           = [];
            $this->stopException        = null;
            $this->suspension           = null;
            
            try {
                $future->complete($stopException);
            } finally {
                $this->callbackId       = '';
                $this->isRunning        = false;
            }
        }
    }
    
    public function run(CoroutineInterface $coroutine): Future
    {
        $this->init();
        
        $callbackId                 = EventLoop::defer(function (string $callbackId)
        use ($coroutine): void {
            
            $suspension             = EventLoop::getSuspension();
            
            if(false === array_key_exists($callbackId, $this->coroutines)) {
                $coroutine->fail(new CoroutineNotStarted);
                return;
            }
            
            $coroutine->defineSuspension($suspension);
            $coroutine->defineSchedulerSuspension($this->suspension);
            
            try {
                $coroutine->resolve($coroutine->execute());
            } catch (\Throwable $exception) {
                
                $coroutine->fail($exception);
                
                if($exception !== $this->stopException) {
                    throw $exception;
                }
                
            } finally {
                $coroutine->resolve();
                unset($this->coroutines[$callbackId]);
                $this->resume();
            }
        });
        
        $this->coroutines[$callbackId] = $coroutine;
        
        if($coroutine->getPriority() >= $this->highestPriority) {
            $this->coroutinesQueue  = [];
        }
        
        return $coroutine->getFuture();
    }
    
    public function awaitAll(Cancellation $cancellation = null): void
    {
        if($this->coroutines === [] || $this->future === null) {
            return;
        }
        
        $this->future->getFuture()->await($cancellation);
    }
    
    public function stopAll(\Throwable $exception = null): void
    {
        $exception                  ??= new CoroutineTerminationException();
        $this->isRunning            = false;
        $this->stopException        = $exception;
        $this->resume();
    }
    
    public function getCoroutinesCount(): int
    {
        return count($this->coroutines);
    }
    
    protected function resume(): void
    {
        if($this->managerResumed) {
            return;
        }
        
        $this->managerResumed       = true;
        $this->suspension?->resume();
    }
}