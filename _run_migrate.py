#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""本番DBマイグレーション実行スクリプト"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from database import migrate_db
    print("  マイグレーション実行中...")
    migrate_db()
    print("  [OK] マイグレーション完了!")
    print("  サービスを再起動してください。")
except Exception as e:
    print("  [ERROR]", e)
    sys.exit(1)
