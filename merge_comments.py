"""
合并所有YouTube视频评论数据到一个文件
方便进行聚类分析
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict

def merge_comments(input_dir: str = "data/youtube", output_file: str = "data/youtube/all_comments_merged.json"):
    """
    合并所有评论文件
    
    Args:
        input_dir: 输入目录
        output_file: 输出文件路径
    """
    input_path = Path(input_dir)
    output_path = Path(output_file)
    
    # 确保输出目录存在
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 获取所有评论文件
    comment_files = sorted(input_path.glob("youtube_comments_*.json"))
    
    if not comment_files:
        print(f"错误: 在 {input_dir} 目录下未找到评论文件")
        return
    
    print(f"找到 {len(comment_files)} 个评论文件")
    print("-" * 50)
    
    # 合并后的数据结构
    merged_data = {
        "total_videos": 0,
        "total_comments": 0,
        "merged_at": datetime.now().isoformat(),
        "videos": [],
        "all_comments": []  # 扁平化的所有评论，方便聚类分析
    }
    
    # 处理每个文件
    for idx, file_path in enumerate(comment_files, 1):
        print(f"[{idx}/{len(comment_files)}] 处理: {file_path.name}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            video_info = data.get("video_info", {})
            comments = data.get("comments", [])
            comments_count = data.get("comments_count", len(comments))
            
            # 添加视频信息
            video_entry = {
                "video_id": video_info.get("video_id", ""),
                "title": video_info.get("title", ""),
                "channel_title": video_info.get("channel_title", ""),
                "published_at": video_info.get("published_at", ""),
                "view_count": video_info.get("view_count", "0"),
                "like_count": video_info.get("like_count", "0"),
                "comment_count": video_info.get("comment_count", "0"),
                "crawled_comments_count": comments_count,
                "crawled_at": data.get("crawled_at", "")
            }
            merged_data["videos"].append(video_entry)
            
            # 处理评论（包括回复）
            for comment in comments:
                video_id = video_info.get("video_id", "")
                
                # 主评论
                main_comment = {
                    "comment_id": comment.get("comment_id", ""),
                    "video_id": video_id,
                    "video_title": video_info.get("title", ""),
                    "author_name": comment.get("author_name", ""),
                    "author_channel_id": comment.get("author_channel_id", ""),
                    "text": comment.get("text", ""),
                    "like_count": comment.get("like_count", 0),
                    "published_at": comment.get("published_at", ""),
                    "updated_at": comment.get("updated_at", ""),
                    "reply_count": comment.get("reply_count", 0),
                    "is_reply": False,
                    "parent_id": None
                }
                merged_data["all_comments"].append(main_comment)
                
                # 处理回复
                replies = comment.get("replies", [])
                for reply in replies:
                    reply_comment = {
                        "comment_id": reply.get("comment_id", ""),
                        "video_id": video_id,
                        "video_title": video_info.get("title", ""),
                        "author_name": reply.get("author_name", ""),
                        "author_channel_id": reply.get("author_channel_id", ""),
                        "text": reply.get("text", ""),
                        "like_count": reply.get("like_count", 0),
                        "published_at": reply.get("published_at", ""),
                        "updated_at": reply.get("updated_at", ""),
                        "reply_count": 0,
                        "is_reply": True,
                        "parent_id": reply.get("parent_id", "")
                    }
                    merged_data["all_comments"].append(reply_comment)
            
            merged_data["total_videos"] += 1
            merged_data["total_comments"] += len(comments)
            
            print(f"  ✓ 视频: {video_info.get('title', 'Unknown')[:50]}...")
            print(f"  ✓ 评论数: {len(comments)}")
            
        except Exception as e:
            print(f"  ✗ 处理文件失败: {e}")
            continue
    
    # 保存合并后的数据
    print("\n" + "=" * 50)
    print("保存合并数据...")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    
    print(f"✓ 合并完成!")
    print(f"  - 总视频数: {merged_data['total_videos']}")
    print(f"  - 总评论数: {len(merged_data['all_comments'])}")
    print(f"  - 输出文件: {output_path.absolute()}")
    
    # 生成统计信息
    print("\n" + "=" * 50)
    print("统计信息:")
    print(f"  - 视频总数: {merged_data['total_videos']}")
    print(f"  - 评论总数（含回复）: {len(merged_data['all_comments'])}")
    
    # 统计主评论和回复
    main_comments = [c for c in merged_data['all_comments'] if not c['is_reply']]
    replies = [c for c in merged_data['all_comments'] if c['is_reply']]
    print(f"  - 主评论数: {len(main_comments)}")
    print(f"  - 回复数: {len(replies)}")
    
    # 按视频统计
    video_stats = {}
    for comment in merged_data['all_comments']:
        video_id = comment['video_id']
        if video_id not in video_stats:
            video_stats[video_id] = 0
        video_stats[video_id] += 1
    
    print(f"\n各视频评论数:")
    for video in merged_data['videos']:
        video_id = video['video_id']
        count = video_stats.get(video_id, 0)
        print(f"  - {video['title'][:40]}... ({count} 条评论)")


if __name__ == "__main__":
    merge_comments()

