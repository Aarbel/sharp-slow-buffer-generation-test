const { promisify } = require('util');
const aws = require('aws-sdk');
const fs = require('fs');
const tmp = require('tmp');

aws.config.update({ region: 'eu-central-1' });
aws.config.setPromisesDependency(require('bluebird'))

const s3 = new aws.S3({
    httpOptions: {
        timeout: 20 * 60 * 1000,
    },
});

/** 
 * Returns the S3 key path for a tile.
 * prefix: Prefix of the S3 keys
 * page: Page number
 */
function getS3TileKeyPath({ prefix, pageNumber, x, y, zoomLevel }) {
    const TileKey = `${prefix}/pages/${pageNumber}/${zoomLevel}/${x}/${y}.png`;
    
    return TileKey;
}




const tmpFile = options => new Promise((resolve, reject) => tmp
    .file(options, (error, path, fd, cleanup) => {
        if (error) {
            reject(error);
        }
        resolve({ path, fd, cleanup });
    }));

const tmpDir = () => new Promise((resolve, reject) => tmp
    .dir((error, path, cleanup) => {
        if (error) {
            reject(error);
        }

        resolve({ path, cleanup });
    }));

async function writeOnDisk(nameOrFd, data) {
    await promisify(fs.writeFile)(nameOrFd, data);

    if (typeof nameOrFd !== 'string') {
        await promisify(fs.close)(nameOrFd);
    }
}

async function writeTmpFileOnDisk(data, options = {}) {
    const { path, fd, cleanup } = await tmpFile(options);

    await writeOnDisk(fd, data);

    return { path, cleanup };
}

const removeFromDisk = filePath => promisify(fs.unlink)(filePath);


module.exports = {
    getS3TileKeyPath,
    s3,
    tmpFile,
    tmpDir,
    writeOnDisk,
    writeTmpFileOnDisk,
    removeFromDisk,
};
