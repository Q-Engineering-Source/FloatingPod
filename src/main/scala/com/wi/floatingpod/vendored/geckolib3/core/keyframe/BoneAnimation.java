/*
 * Copyright (c) 2020.
 * Author: Bernie G. (Gecko)
 */

package com.wi.floatingpod.vendored.geckolib3.core.keyframe;

import com.wi.floatingpod.vendored.mclib.math.IValue;

public class BoneAnimation {
	public String boneName;
	public VectorKeyFrameList<KeyFrame<IValue>> rotationKeyFrames;
	public VectorKeyFrameList<KeyFrame<IValue>> positionKeyFrames;
	public VectorKeyFrameList<KeyFrame<IValue>> scaleKeyFrames;
}
