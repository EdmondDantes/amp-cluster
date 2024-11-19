<?php
declare(strict_types=1);

use Amp\Sync\Channel;

return function (Channel $channel): void {

    $address                        = $channel->receive(new \Amp\TimeoutCancellation(5));
    $result                         = file_get_contents($address);

    if ($result === false) {
        $channel->send('Failed to get content');
    }

    $channel->send($result);
};
