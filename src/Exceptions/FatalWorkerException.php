<?php
declare(strict_types=1);

namespace CT\AmpServer\Exceptions;

/**
 * FatalWorkerException is thrown when a worker encounters a fatal error.
 * and all clues should be stopped.
 */
final class FatalWorkerException    extends RemoteException
{
    
}