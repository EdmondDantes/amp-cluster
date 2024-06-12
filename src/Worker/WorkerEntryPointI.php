<?php
declare(strict_types=1);

namespace CT\AmpServer\Worker;

interface WorkerEntryPointI
{
    public function initialize(WorkerI $workerStrategy): void;
    public function run(): void;
}