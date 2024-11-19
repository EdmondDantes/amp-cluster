<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Internal;

/**
 * @internal
 */
final class Safe
{
    /**
     * @throws \Error
     */
    public static function execute(callable $callback, ?callable $throwConstructor = null): mixed
    {
        $error                      = null;

        \set_error_handler(
            function ($severity, $message) use (&$error, $throwConstructor) {

                if ($throwConstructor !== null) {
                    $error          = \call_user_func($throwConstructor, $message, $severity);
                } else {
                    $error          = new \Error($message);
                }
            }
        );

        try {
            $result                 = \call_user_func($callback);
        } finally {
            \restore_error_handler();
        }

        if ($error !== null) {
            throw $error;
        }

        return $result;
    }
}
