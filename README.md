
<p align="center"> <img src="https://lh3.googleusercontent.com/fife/ALs6j_EkNnbTexbzHn0TvEmg83gI16ZTkw8B6F-3eXtVqhoibLRYzUDIrfoN7Nekxh-dqgEdJPPuF_kbiC3wMhModyAjKCz-8BAO91ADZUiehixgzipdnxf-QRZhIrUpZnv4hXD6aDKatCBW_U7i1D4dwxJSKQbGhCk2kw-DvyFGDNj7U9-FLNby1S_49Qi5-NN-W6JQHA-5eY3pKZMvRhY38tc7SbidSQXxEBOiyMJ_WxN5Qrl71JIZr15i8sn8MEzU7mWXB8EeV9rtxAVj8bDEfqhd3ZvRZM9rFcB2LbV-7Ce1pD1PI_HVyslQ01eWrJtUloqWWJPSNVNQlL1mSK_4a-RsdYBKs2D7Yqpve_sTwlnGJdTFbM8M5w6wYOilGiYn24Fu1aRL7VZxC0Pm5NzcJYl1L32uAJbhJHzpygqQV5Rrb7fd-W_XD13P0kgENzfAf_G6dC9co3vLgmqWF5MGMgQ0orkJh3aqrk9yay8pSsXVurG1zcQFLZa3KK5dNWRIIvuupr8xWNtqZicacwhhTvoFZOPsdFA_yNPGFC2AO1in2WqXnuhvu33pJIM8dB-JdpeS9682MYNXWEJp3nlN5bJ62GzD7YiX4AeRNMK1t6Wh7TW8plXpEtVZ1BIetYEVJpt-9trymWVwvNEoNpK1K8dAhKFV6ndWe-K9YjU1bymJUjnoxCW619SPWsl7agLQebnIcPS2Tc21fSVYZLloxVegO7M78R4d70_ty0zRCuAh7ngi9NPgcgSaMwiHMEAkD7NQWpsDLDdpqBQC_cZczRsv5qMTFbuFPWZc1kGTSTHoKydExdleHBp8qzwERftkDGKKZ_9IQ9Ygto1V80XBHTqQPIFcjlHhHdNnLU6F-SbFs5aUztklaQUIKIRfNou80lQEIuMs2NoZqHSvXilu3wD_cebfX2b8V6bLzkM7V2z3DKkrILeDFJ42F2cIMmHfCl4GSEMeJGGwxOkYeAMaYzfMm_46-9Ns3ZUPA_ldQ6g0B2Ax8AEDkIfeMEMFRgX9KJXKGy2XL9q738d1A-i-F09j_hKtDX0eNz_vApNe6UdEhobzI-uon099bVfqEiaeeuU02jbbDyBjy6Nq_NGFwTqlXiqVS00xcAFBLsrUzvz6wZJtolM7sTcWYU-WVMqY4qApJlk8ugFGHb3sKvwJ4HfFh1SDvMvJaXPNeClH6-dUTjm-y807WwKeWvk-KffxZzf2rupiv8QdIlJ90ur5NYWlnjO0KjEcODkAE88KlPzIN_OK5DwDn7vh9qNOvKgSYvkec1MlX2ehft2-Ch9BlPHy0pjf7WZNpZkXJDYGEQJOi1G5Ok_Y0DHLxHy7od_PcxkrhH-qipSDMaqE4_G-4bJOAA4s-0FOyvxn7-FTeKGPY6b7QJ4W0lJmjhYO0m9RqDRJu--RIjVaIfz77z7XJrHKpcKRwQa5P-L3TfEQnUgKTIUXSrCTpkmnK5gDw0lyCSOW0eQZolxGGzqdjRNxjWcPU6mD1oGJS6bMYIqAZinIN8qAKAju3AGf1_X-cJyoL3_-M5Th9qZxJId3l5MAGC0CNyRp0E6ZC1wmfOZHFR26t8_kB6ixXMixi8BUFR7rgmoJPnen9ARu7fVPBL0mx5xDsOt-3coKzwqVVG3EbNOa0mjRXV9IdV2DwNP2pAXajSge3hhHPEwLOPuG-OpfpdgmUUx28kv9N77pZ7mn35ry9g=w3840-h1694" alt="RAF Banner" width="100%"> </p>

<p align="center">
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-gre.svg" alt="License: MIT">
  </a>
  <a href="https://www.python.org/downloads/">
    <img src="https://img.shields.io/badge/python-3.8+-blue.svg" alt="Python 3.8+">
  </a>
</p>


RADICAL AsyncFlow (RAF) is a fast asynchronous scripting library built on top of [asyncio](https://docs.python.org/3/library/asyncio.html) for complex async/sync workflows on HPC, clusters, and local machines. It supports pluggable execution backends with intuitive task dependencies and workflow composition. Currently, RAF supports the following execution backends:

- [Radical.Pilot](https://radicalpilot.readthedocs.io/en/stable/#)
- [Dask.Parallel](https://docs.dask.org/en/stable/)
- [ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)
- Noop with `dry_run`
- Custom implementations



## Basic Usage
```python
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend

async def main():
    # Create backend and workflow
    backend = RadicalExecutionBackend({'resource': 'local.localhost'})
    flow = WorkflowEngine(backend=backend)

    @flow.executable_task
    def task1():
        return "echo $RANDOM"

    @flow.function_task
    def task2(t1_result):
        return t1_result * 2 * 2

    # create the workflow
    t1_result = task1().result()
    t2_future = task2(t1_result) # t2 depends on t1 (waits for it)

    t2_result = t2_future.result()

    # shutdown the execution backend
    await flow.shutdown()
```