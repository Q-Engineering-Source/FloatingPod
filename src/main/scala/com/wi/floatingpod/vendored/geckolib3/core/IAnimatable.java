/*
 * Copyright (c) 2020.
 * Author: Bernie G. (Gecko)
 */
package com.wi.floatingpod.vendored.geckolib3.core;

import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationData;
import com.wi.floatingpod.vendored.geckolib3.core.manager.AnimationFactory;

/**
 * This interface must be applied to any object that wants to be animated
 */
public interface IAnimatable {
	void registerControllers(AnimationData data);

	AnimationFactory getFactory();
}
