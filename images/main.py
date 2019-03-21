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

def keypoint2dict(kp):
    return {
        "x": kp.pt[0],
        "y": kp.pt[1],
        "size": kp.size,
        "angle": kp.angle,
        "response": kp.response,
        "octave": kp.octave,
        "class_id": kp.class_id
    }

# Will probably be useful later
def dict2keypoint(d):
    return cv2.KeyPoint(
        d["x"],
        d["y"],
        d["size"],
        d["angle"],
        d["response"],
        d["octave"],
        d["class_id"]
    )

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
        orb = cv2.ORB_create()

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
                    except HTTPError as e:
                        print("Error downloading %s: %r" % (url, e))
                        time.sleep(5)

                filename = posixpath.join(colldir, url[(url.rfind("/") + 1):])

                with sftp.open(filename, "wb") as fd:
                    fd.write(filedata)

                print("Downloaded %s from tweet %d to %s" % (url, r0["id"], filename))

                sha256sum = hashlib.sha256()
                sha256sum.update(filedata)

                r = {
                    "remote_url": url,
                    "local_url": filename,
                    "sha256sum": sha256sum.hexdigest(),
                    "keypoints": None,
                    "descriptors": None
                }

                with tempfile.NamedTemporaryFile(delete = False) as fd:
                    tempname = fd.name
                    fd.write(filedata)

                img = cv2.imread(tempname, cv2.IMREAD_GRAYSCALE)
                if img is not None:
                    kp, des = orb.detectAndCompute(img, None)

                    r["keypoints"] = list(map(keypoint2dict, kp))
                    r["descriptors"] = des.tolist()

                os.remove(temp_filename)
                medialist.append(r)

            coll_to.insert_one({
                "id": r0["id"],
                "media": medialist
            })
