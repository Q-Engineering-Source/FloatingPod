/*
 * 改自 GeckoLib 3.0.31：迁移骨骼树节点的包名和 Bone import 到 FloatingPod 命名空间。
 * 该调整保持层级聚合结构，供本地 geo 构建器消费而不加载外部 GeckoLib。
 */
package com.wi.floatingpod.vendored.geckolib3.geo.raw.tree;
import com.wi.floatingpod.vendored.geckolib3.geo.raw.pojo.Bone;
import java.util.HashMap;
public class RawBoneGroup
{
	public HashMap<String, RawBoneGroup> children = new HashMap<>();
	public Bone selfBone;
	public RawBoneGroup(Bone bone)
	{
		this.selfBone = bone;
	}
}
