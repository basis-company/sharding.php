<?php

declare(strict_types=1);

namespace Basis\Sharding\Attribute;

use Attribute;
use Carbon\Carbon;

#[Attribute(Attribute::TARGET_CLASS)]
class Caching
{
    public function __construct(
        public int|string $ttl = 60,
    ) {
    }

    public function getLifetime(): int
    {
        $ttl = $this->ttl;

        if (is_string($ttl)) {
            $timestamp = time();
            $ttl = Carbon::createFromTimestamp($timestamp)
                ->add($this->ttl)
                ->getTimestamp() - $timestamp;
        }

        return $ttl;
    }
}
