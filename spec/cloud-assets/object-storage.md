# Object Storage

The primary goal is to be able to calculate the total cost of ownership(TCO) for Object Storage buckets.
The cost _must_ be represented as a Prometheus metric that is exported.
The metrics should represent the hourly cost for the dimension that is being calculated.

## Introduction

Object Storage is a service that allows you to store and retrieve any amount of data at any time, from anywhere on the web. 
You can use Object Storage to host static websites, store files for distributed access, or use it as a data store for your applications.

Each provider has a different way of calculating the total cost of ownership(TCO) for buckets that are created.
Our attempt here is to generalize the TCO for buckets and provide a formula that can be used to calculate the TCO for buckets.

## Types of Cost

- Storage 
- Requests
- Data Transfer
- Data Retrieval
- Management
- Other

Total Cost of Ownership(TCO) = Storage + Requests + Data Transfer + Data Retrieval + Management + Other

### Storage

Storage is the cost per GiB of the data in the bucket.
Storage must be represented as currency per GiB(2^30 bytes) per hour.
Each region will have a different price.
There is also an extra cost if the data is replicated across multiple regions.
Some providers allow two regions, some allow multiple regions.
Each CSP has different tiers of storage available depending on how frequently the data is accessed.
Each CSP has a slightly nuanced take on which tier is which, so it is in our best interest to normalize the tiers in a way that is vendor agnostic.

One way we can normalize the tiers:
- Standard
- Infrequent
- Cold
- Archive

AWS has the following tiers:
- Standard
- Intelligent Tiering
- Standard-IA
- One Zone-IA
- Glacier
- Glacier Deep Archive
- Reduced Redundancy

GCP has teh following tiers:
- Standard
- Nearline
- Coldline
- Archive

Azure has the following tiers:
- Hot
- Cool
- Archive
- Premium

Possible labels:
- region
- tier
- replication_factor

### Requests

Each CSP has a rate limit on the number of requests that can be made to the bucket.
These are usually represented by different teirs of pricing.
- GCP: [gcp requests]
- AWS: [aws requests]
- Azure: [azure requests]

### Data Transfer

### Data Retrieval

### Management

### Other

[gcp requests](https://cloud.google.com/storage/pricing#operations-pricing)
[aws requests](https://aws.amazon.com/s3/pricing/)
[azure requests](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/#pricing)
[aws reduced redundancy](https://aws.amazon.com/s3/reduced-redundancy/)