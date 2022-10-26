#!/bin/sh

VERSION=0.1.0
echo "Creating build directory"

if [ -d "./build" ]
then 
    echo "Build directory exists, skipping"
else
    mkdir build
fi

echo "Packaging depencies"
cd braze_import_objects_lambda
pip install --target ./package requests tenacity
echo "Packaging the app"
cd package
zip -r ../braze-import-objects-lambda-v"$VERSION".zip .
cd ..
zip -g braze-import-objects-lambda-v"$VERSION".zip lambda_function.py
mv braze-import-objects-lambda-v"$VERSION".zip ../build
rm -r package
echo "Done"