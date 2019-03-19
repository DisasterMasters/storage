import contextlib
from email.utils import format_datetime
import hashlib
import posixpath
import sys
from urllib.error import HTTPError
from urllib.request import urlopen
import tempfile
import time

import cv2
import tweepy

from common import *
from points import get_orb, get_sift, get_surf

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: %s <collection_from> <collection_to>", file = sys.stderr)
        exit(-1)

    with contextlib.ExitStack() as exitstack:
        sftp = exitstack.enter_context(opentunnel())
        db = exitstack.enter_context(opendb())
        coll_from = exitstack.enter_context(opencoll(db, sys.argv[1]))
        coll_to = exitstack.enter_context(opencoll(db, sys.argv[2]))

        colldir = posixpath.join("/", "home", "nwest13", "Media", sys.argv[2][(sys.argv[2].find("_") + 1):])

        try:
            sftp.mkdir(colldir)
        except IOError:
            pass

        cursor = coll_from.find(
            {"$or": [{"entities.media": {"$exists": True}}, {"extended_tweet.entities.media": {"$exists": True}}]},
            ["id", "entities.media", "extended_tweet.entities.media"],
            no_cursor_timeout = True
        )

        cursor = exitstack.enter_context(contextlib.closing(cursor))

        for r0 in cursor:
            medialist = []
            urls = set()

            if "media" in r0["entities"]:
                urls |= {media["media_url"] for media in r0["entities"]["media"]}

            if "media" in r0["extended_tweet"]["entities"]:
                urls |= {media["media_url"] for media in r0["extended_tweet"]["entities"]["media"]}

            assert len(urls) > 0

            for url in urls:
                while True:
                    try:
                        with urlopen(url) as response:
                            filedata = response.read()
                        break
                    except HTTPError:
                        time.sleep(5)

                filename = posixpath.join(colldir, url[(url.rfind("/") + 1):])

                with sftp.open(filename, "wb") as fd:
                    fd.write(filedata)

                print("Downloaded %s from tweet %d to %s" % (url, r0["id"], filename))

                with tempfile.NamedTemporaryFile() as fd:
                    fd.write(filedata)
                    mat = cv2.imread(fd.name, cv2.IMREAD_GRAYSCALE)

                sha256sum = hashlib.sha256()
                sha256sum.update(filedata)

                medialist.append({
                    "remote_url": url,
                    "local_url": filename,
                    "sha256sum": sha256sum.hexdigest(),
                    "orb": get_orb(mat) if mat is not None else None,
                    "sift": get_sift(mat) if mat is not None else None,
                    "surf": get_surf(mat) if mat is not None else None
                })

            coll_to.insert_one({
                "id": r0["id"],
                "media": medialist
            })
