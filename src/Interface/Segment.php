<?php

namespace Basis\Sharding\Interface;

interface Segment
{
    public static function getSegment(): string;
}
