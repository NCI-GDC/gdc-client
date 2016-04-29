# -*- mode: python -*-
a = Analysis(['gdc-client'],
             pathex=['E:\\gdc-client\\bin'],
             hiddenimports=[],
             hookspath=['.'],
             runtime_hooks=None)
pyz = PYZ(a.pure)
a.datas = list({tuple(map(str.upper, t)) for t in a.datas})
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='gdc-client.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True , icon='..\\resources\\gdc_client.ico')
