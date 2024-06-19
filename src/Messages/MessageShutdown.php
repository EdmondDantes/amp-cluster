<?php
declare(strict_types=1);

namespace CT\AmpPool\Messages;

/**
 * Request shutdown the worker.
 *
 * @package CT\AmpPool\Messages
 */
final readonly class MessageShutdown
{
    public function __construct(public bool $afterLastJob = true) {}
}