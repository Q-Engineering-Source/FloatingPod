/*
 * Copyright (c) 2020.
 * Author: Bernie G. (Gecko)
 */

package com.wi.floatingpod.vendored.geckolib3.core.builder;

import java.util.ArrayList;
import java.util.List;

import com.wi.floatingpod.vendored.geckolib3.core.builder.ILoopType.EDefaultLoopTypes;
import com.wi.floatingpod.vendored.geckolib3.core.keyframe.BoneAnimation;
import com.wi.floatingpod.vendored.geckolib3.core.keyframe.EventKeyFrame;
import com.wi.floatingpod.vendored.geckolib3.core.keyframe.ParticleEventKeyFrame;

/**
 * A specific animation instance
 */
public class Animation {
	public String animationName;
	public Double animationLength;
	public ILoopType loop = EDefaultLoopTypes.LOOP;
	public List<BoneAnimation> boneAnimations;
	public List<EventKeyFrame<String>> soundKeyFrames = new ArrayList<>();
	public List<ParticleEventKeyFrame> particleKeyFrames = new ArrayList<>();
	public List<EventKeyFrame<String>> customInstructionKeyframes = new ArrayList<>();

}
