<?php

namespace Basis\Sharding\Trait;

use BadMethodCallException;

trait References
{
    use ShardingDatabase;

    public function __call(string $name, array $arguments)
    {
        $getter = substr($name, 0, 3);
        if ($getter === 'get' && $alias = substr($name, 3)) {
            if (substr($alias, -10) == 'Collection' && $alias = substr($alias, 0, -10)) {
                if ($collection = $this->shardingDatabase->schema->getCollection(get_class($this), $alias)) {
                    return $this->shardingDatabase->find($collection['destination'], [$collection['property'] => $this->id]);
                }
            } elseif ($reference = $this->shardingDatabase->schema->getReference(get_class($this), $alias)) {
                if ($this->{$reference['property']}) {
                    return $this->shardingDatabase->findOne($reference['class'], ['id' => $this->{$reference['property']}]);
                }
                return null;
            }
        }

        throw new BadMethodCallException("Method $name not found");
    }
}
