# tapalcatl-js

This serves as a reference implementation of [Tapalcatl
2](https://medium.com/@mojodna/tapalcatl-cloud-optimized-tile-archives-1db8d4577d92)
for JavaScript. It includes support for block-aligned reads (and subsequent
caching) of remote Tapalcatl archives.

## Tuning

The default block size for fetching remote (HTTP(S), S3) sources is 1MB. To
change it, set `DEFAULT_BLOCK_SIZE` (in bytes) in the server's environment.

## License

tapalcatl-js is availabile under the ISC License. See [LICENSE](LICENSE)
file for more details
