import contextlib
import datetime
from email.utils import format_datetime
import hashlib
import posixpath
import sys
from urllib.error import HTTPError
from urllib.request import urlopen
import tempfile
import time

import cv2

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

def download(sftp, remote_url, local_url):
    # Python is a very good language because of its clear syntax, you'll
    # definitely NEVER need four or five try/catch blocks in it

    try:
        with sftp.open(local_url, "rb") as fd:
            print("%s already exists, skipping" % local_url)
            return fd.read()
    except IOError:
        pass

    while True:
        try:
            with urlopen(remote_url) as response:
                filedata = response.read()

            break

        except HTTPError as err:
            print("Error downloading %s: %r" % (url, e), end = "")

            if err.code // 100 == 4:
                print("Skipping")
                return None

            else:
                print("Sleeping")
                time.sleep(5)

    with sftp.open(local_url, "wbx") as fd:
        fd.write(filedata)
        print("Downloaded %s to %s" % (remote_url, local_url))

    return filedata

def extract_kp_des(orb, filedata):
    try:
        with tempfile.NamedTemporaryFile(delete = False) as fd:
            filename = fd.name
            fd.write(filedata)

        img = cv2.imread(filename, cv2.IMREAD_GRAYSCALE)
    finally:
        os.remove(filename)

    if img is not None:
        kp, des = orb.detectAndCompute(img, None)

        kp = list(map(keypoint2dict, kp))
        des = des.tolist() if des is not None else None
    else:
        kp = None
        des = None

    return kp, des

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
            atimelist = []
            urls = set()

            if "media" in r0["entities"]:
                urls |= {media["media_url"] for media in r0["entities"]["media"]}

            if "media" in r0["extended_tweet"]["entities"]:
                urls |= {media["media_url"] for media in r0["extended_tweet"]["entities"]["media"]}

            assert len(urls) > 0

            for url in urls:
                filename = posixpath.join(colldir, url[(url.rfind("/") + 1):])

                filedata = download(sftp, url, filename)
                if filedata is None:
                    continue

                retrieved_at = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)

                sha256sum = hashlib.sha256()
                sha256sum.update(filedata)

                kp, des = extract_kp_des(orb, filedata)

                medialist.append({
                    "remote_url": url,
                    "local_url": filename,
                    "retrieved_at": retrieved_at,
                    "sha256sum": sha256sum.hexdigest(),
                    "keypoints": kp,
                    "descriptors": des
                })

            if medialist:
                print("Adding entry for tweet %d with %d media entries" % (r0["id"], len(medialist)))

                coll_to.insert_one({
                    "id": r0["id"],
                    "retrieved_at": max(media["retrieved_at"] for media in medialist),
                    "media": medialist
                })
