from operator import attrgetter
name = attrgetter('name')
from urlparse import urlparse
import os

import boto
import trovebox

import settings

s3 = boto.connect_s3(settings.S3_ACCESS_KEY, settings.S3_ACCESS_SECRET)
bucket = s3.get_bucket(settings.S3_BUCKET)

client = trovebox.Trovebox()
albums = dict((name(album), album) for album in client.albums.list())

PHOTO_EXTENSIONS = getattr(settings, 'PHOTO_EXTENSIONS', ['jpg', 'png'])

for key in bucket.list(settings.S3_UPLOAD_FOLDER):

    base, ext = os.path.splitext(key.name)
    # only photos
    if not ext.lower() in PHOTO_EXTENSIONS: continue

    # set up an album if required.
    album = os.path.basename(os.path.dirname(key.name))
    if album != key.name and not album in albums:
        albums[album] = client.album.create(album)
        album = albums[album]
    else:
        album = albums.get(album)
        
    url = key.generate_url(expires_in=300, query_auth=False, force_http=True)
    try:
        photo = client.photo.upload_from_url(url)
        if album:
            client.album.add(album, photo)
    except trovebox.errors.TroveboxDuplicateError:
        # with duplicates we just delete the photo and move on.
        key.delete()
        continue

    key.delete()

for key in bucket.list(settings.S3_UPLOAD_FOLDER):
    if key.name == settings.S3_UPLOAD_FOLDER + '/': continue
    key.delete()
