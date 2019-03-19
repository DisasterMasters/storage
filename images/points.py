import cv2

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

def __get_points(mat, method):
    kp, des = method.create().detectAndCompute(mat, None)

    return {
        "keypoints": list(map(keypoint2dict, kp)),
        "descriptors": des.tolist()
    }

def get_orb(mat):
    return __get_points(mat, cv2.ORB)

def get_sift(mat):
    return __get_points(mat, cv2.xfeatures2d_SIFT)

def get_surf(mat):
    return __get_points(mat, cv2.xfeatures2d_SURF)
