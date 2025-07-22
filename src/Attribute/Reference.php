<?php

declare(strict_types=1);

namespace Basis\Sharding\Attribute;

use Attribute;

#[Attribute(Attribute::TARGET_PROPERTY | Attribute::TARGET_PARAMETER)]
class Reference
{
    public static function create(string $class, string $property, string $destination): self
    {
        return (new self($destination))->setSource($class, $property);
    }

    public string $model = '';
    public string $property = '';

    public function __construct(
        public string $destination,
    ) {
    }

    public function setSource(string $class, string $property): self
    {
        $this->model = $class;
        $this->property = $property;

        return $this;
    }
}
