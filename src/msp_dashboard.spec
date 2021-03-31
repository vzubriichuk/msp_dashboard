# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

from PyInstaller.utils.hooks import collect_submodules
scipy = collect_submodules('scipy.stats')
sqlalchemy = collect_submodules('sqlalchemy')
all_hidden_imports = sqlalchemy + scipy

a = Analysis(['msp_dashboard.py'],
             pathex=['C:\\Work\\Python_projects\\MSP_Dashboard\\src'],
          binaries=[],
             datas=[( '.\\resources\\*.ico', 'resources' ),
                    ( '.\\resources\\*.xlsx', 'resources' )],
             hiddenimports=all_hidden_imports,
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='msp_dashboard',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True , icon='resources\\msp.ico')
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='msp_dashboard')
