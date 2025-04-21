# Sharded.php - A Sharded Library for PHP

## Overview

Sharded is a php library designed to handle domain data that is split into segments, where each segment represents a collection of entities. The library provides mechanisms for persisting these segments across distributed storage systems using a bucket-based approach.

## Key Concepts

### Domain Segmentation
- The domain is divided into logical **segments**, each containing a collection of entities
- Segments allow for horizontal partitioning of data

### Persistence Model
- Segment persistence is achieved through **buckets**
- Buckets are distributed across multiple storage backends
- Provides fault tolerance and scalability

## Features

- **Shard management**: Easily create and manage data shards
- **Bucket distribution**: Automatic distribution of buckets across storage systems
- **Entity operations**: CRUD operations for entities within segments
- **Storage abstraction**: Support for multiple storage backends
- **Scalability**: Designed to handle large-scale data distribution

## Installation

```bash
composer require basis-company/sharded
```

## Contributing

Contributions are welcome! Please see our [Contribution Guidelines](https://github.com/basis-company/sharded.php/blob/master/CONTRIBUTING.md).

## License

Sharded.php is open-source software licensed under the [MIT License](https://github.com/basis-company/sharded.php/blob/master/LICENSE).

## Support

For support and questions, please open an issue on our [GitHub repository](https://github.com/basis-company/sharded.php/issues).