const _ = require('lodash');
const BluebirdPromise = require("bluebird");
const fs = require('fs');
const sharp = require('sharp');
const streamifier = require('streamifier');
const streamLength = require("stream-length");
const lengthStream = require('length-stream');
const stream = require('stream');
const isStream = require('is-stream');
const { readSync, writeSync } = require('image-js');
const fsExtra = require('fs-extra')


const { getS3TileKeyPath, promisifedS3Methods, s3 } = require('./utils');

/* The source should be already resized.
   size: The width and the height of the output image. */
const getFilePath = ({
    zoomLevel,
    x,
    y,
}) => {
    return `./dist/img-clovis-${zoomLevel}-${x}-${y}.png`
}

/* Returns all the tile transformers for a given zoom level. */
const zoomLevelTransformers = (bigSquareImage, zoomLevel, tileSize) => {
    /* rootSize = 4096px x 4096px if zoomLevel is 3
       rootSize = 8192px x 8192px if zoomLevel is 4 
       So be carefull with the RAm and cpu 
    */
    const rootSize = tileSize * 2 ** zoomLevel;

    console.log('rootSize :', rootSize);

    /* Resize the BigSquare image to root size of zoom level */
    const zoomRoot = bigSquareImage.resize({
        interpolationType: "BILINEAR", // or "BILINEAR" or "NEAREST", in order of decreasing quality
        width: rootSize,
        preserveAspectRatio: true // Or height: number
    })

    const zoomRootPath = `./dist/img-clovis-zoomRoot-${zoomLevel}.png`
    writeSync(zoomRootPath, zoomRoot);

    console.log('zoomLevelTransformers :', zoomLevel);

    const tileColumnCount = 2 ** zoomLevel;

    const allTiles = _().range(tileColumnCount)
        .flatMap(x => _.range(tileColumnCount)
            .map(y => {
                const zoomRootClone = zoomRoot;
                const cropParams = {
                    x: x * tileSize, 
                    y: y * tileSize, 
                    width: tileSize, 
                    height: tileSize,
                }

                const cropedTile = zoomRootClone
                    .crop(cropParams)

                const tilePath = getFilePath({
                    zoomLevel,
                    x,
                    y,
                })

                cropedTile.save(tilePath);

                const newTile = {
                    x,
                    y,
                    path: tilePath,
                    zoomLevel
                }

                return newTile;
            })
        )
        .value();

    return allTiles
};


exports.lambdaHandler = async (event) => {
    // console.log('CLEAN FOLDER :');
    // fsExtra.emptyDirSync('./dist');
    // console.log('CLEAN FINISHED :');

    const { body } = event;
    console.log('Lambda body :', body);
    const { 
        blueprintsBucket, // Bucket of the blueprints
        fileBucketKeyPrefix,  // Keyprefix of the folder of the file in blueprint bucket
        pageNumber, // Page number of the pdf
        tileSize, // Size of a tile
        zoomLevel, // Total zoom levels allowed (saved in the file)
    } = body;

    console.log('>>> blueprintsBucket :', blueprintsBucket);
    console.log('>>> fileBucketKeyPrefix :', fileBucketKeyPrefix);



    /* Fetch the big square image from S3 */
    const bigImagePath = `${fileBucketKeyPrefix}/pages/${pageNumber}/big.png`;
    
    await s3.getObject({
            Bucket: blueprintsBucket,
            Key: bigImagePath,
        })
        .promise()
        .then((data) => {
            console.log('reponse data :', data);
            fs.writeFile('./dist/big.png', data.Body, function(err){
                if (err) {
                    console.log('File creation error');
                    console.log(err.code, "-", err.message);

                } else {
                    console.log('File creation success');
                }
            });
        })
        .catch((err) => {
            console.log('>>> Upload error from S3 :', err);
        })

    console.log('>>>> Convert image :');

    const bigTempImage = readSync("big.png");

    const zoomLevelRange = _.range(zoomLevel);

    console.log('>>>> GO TILES 2 :');
    let zoomLevelsTiles = [];
    zoomLevelRange.map((zoomLevel) => {
        const newTiles = zoomLevelTransformers(bigTempImage, zoomLevel, tileSize)
        zoomLevelsTiles.push(...newTiles);
    })
    console.log('>>>> WOHOOO TILES FINISHED :');
    console.log('>>>> zoomLevelsTiles :', zoomLevelsTiles);

    let promisesResult;
    await BluebirdPromise.all(
        zoomLevelsTiles.map(({ x, y, path, zoomLevel }) => {
                const tilePath = path
                const fileContent = fs.readFileSync(tilePath);
                const s3Path = getS3TileKeyPath({ prefix: fileBucketKeyPrefix, pageNumber, x, y, zoomLevel})

                const tileToUpload = {
                    Bucket: blueprintsBucket,
                    Key: s3Path,
                    Body: fileContent,
                    ACL: 'public-read',
                    ContentType: 'image/png',
                };
                
                const uploadPromise = s3.upload(
                    tileToUpload,
                    {
                        partSize: 5 * 1024 * 1024,
                        queueSize: 10,
                    },
                ).promise()
                
                return uploadPromise.then(function(data) {
                    console.log('>>> Success upload tilePath:', tilePath);
                    console.log('>>> Success upload s3Path:', s3Path);
                    console.log('');
                }).catch(function(err) {
                    console.log('>>> Download error from S3:', s3Path);
                    console.log('>>> Download error from S3:', err);
                });
            }
        ),
        
    ).then((result) => {
        promisesResult = "Success"
    }).catch(err => {
        console.error(err);
        promisesResult = "Error"
    })

    return {
        body: promisesResult,
        statusCode: 200,
    };
};
