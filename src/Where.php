<?php

declare(strict_types=1);

namespace Basis\Sharding;

class Where
{
    public null|int|string $equals = null;
    public null|int|string $isGreaterThan = null;
    public null|int|string $isLessThan = null;
    public function __construct(
        public readonly Select $select,
        public readonly ?Where $parent = null,
    ) {
    }

    /**
     * @return Where[]
     */
    public function getConditions(): array
    {
        return array_merge([$this], $this->parent ? $this->parent->getConditions() : []);
    }

    public function equals(int|string $value): Select
    {
        $this->equals = $value;
        return $this->select;
    }

    public function isGreaterThan(int|string $value): Select
    {
        $this->isGreaterThan = $value;
        return $this->select;
    }

    public function isLessThan(int|string $value): Select
    {
        $this->isLessThan = $value;
        return $this->select;
    }
}
