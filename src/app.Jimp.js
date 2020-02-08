const _ = require('lodash');
const BluebirdPromise = require("bluebird");
const fs = require('fs');
const sharp = require('sharp');
const streamifier = require('streamifier');
const streamLength = require("stream-length");
const lengthStream = require('length-stream');
const stream = require('stream');
const isStream = require('is-stream');
const { Image } = require('image-js');
const fsExtra = require('fs-extra');
const Jimp = require('jimp');

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
const zoomLevelTransformers = async (jimpBigImage, zoomLevel, tileSize) => {
    /* rootSize = 4096px x 4096px if zoomLevel is 3
       rootSize = 8192px x 8192px if zoomLevel is 4 
       So be carefull with the RAm and cpu 
    */
    console.log('>>>> 1 :');
    console.log('zoomLevelTransformers :', zoomLevel);
    const rootSize = tileSize * 2 ** zoomLevel;

    /* Resize the BigSquare image to root size of zoom level */
    const zoomRootPath = `./dist/img-clovis-zoomRoot-${zoomLevel}.png`;
    console.log('>>>> 1 newImage:', jimpBigImage);
    const newImage = jimpBigImage
        .resize(rootSize, rootSize) // resize
        .quality(80) // set JPEG quality
        .write(zoomRootPath); // save

    console.log('>>>> 2 newImage:', newImage);
    console.log('>>>> 2 jimpBigImage:', jimpBigImage);
    
    const jimpZoomRootImage = await Jimp.read(zoomRootPath);
    console.log('>>>> 3 :');

    const tileColumnCount = 2 ** zoomLevel; 



    // const allTiles = _().range(tileColumnCount)
    //     .flatMap(x => _.range(tileColumnCount)
    //         .map(y => {
    //             const tilePath = getFilePath({
    //                 zoomLevel,
    //                 x,
    //                 y,
    //             })

    //             jimpZoomRootImage
    //                 .crop(x * tileSize, y * tileSize, tileSize, tileSize) // resize
    //                 .write(tilePath); // save2

    //             const newTile = {
    //                 x,
    //                 y,
    //                 path: tilePath,
    //                 zoomLevel
    //             }

    //             console.log('generated : ', tilePath);

    //             return newTile;
    //         })
    //     )
    //     .value();

    return []
};


exports.lambdaHandler = async (event) => {
    console.log('CLEAN FOLDER :');
    fsExtra.emptyDirSync('./dist');
    console.log('CLEAN FINISHED :');

    const { body } = event;
    console.log('Lambda body :', body);
    const { 
        blueprintsBucket, // Bucket of the blueprints
        fileBucketKeyPrefix,  // Keyprefix of the folder of the file in blueprint bucket
        pageNumber, // Page number of the pdf
        tileSize, // Size of a tile
        zoomLevelCount, // Total zoom levels allowed (saved in the file)
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

    const jimpBigImage = await Jimp.read('./dist/big.png');

    const zoomLevelRange =_.range(zoomLevelCount);

    console.log('>>>> GO TILES :');
    let zoomLevelsTiles = [];
    await BluebirdPromise.mapSeries(zoomLevelRange, async (zoomLevel) => {
            console.log('>>>> AAAA :');
            const newTiles = await zoomLevelTransformers(jimpBigImage, zoomLevel, tileSize)
            console.log('>>>> BBBB newTiles:', newTiles);
            zoomLevelsTiles.push(...newTiles);
        }
    )
    console.log('>>>> WOHOOO TILES FINISHED :');
    console.log('>>>> zoomLevelsTiles :', zoomLevelsTiles);


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
        console.log('>>>> WOHOOO:');
        console.log('>>>>> BluebirdPromise.mapSeries result:', result);
    }).catch(function(err) {
        console.log('>>> BluebirdPromise.mapSerieserror from S3 :', err);
    });


    return "SUCCESS"
};
