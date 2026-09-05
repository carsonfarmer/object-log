"""Opt-in unchanged Git URI clients, authenticated range resume and cold GC.

Uses the partial fixture's loopback provider configuration. Imported helpers do
not start providers. Native prestarted MinIO and pinned owned Docker both work.
"""
import base64
import hashlib
import json
import pathlib
import secrets
import socket
import subprocess
import tempfile
import urllib.error
import urllib.request
import uuid

from check_partial import ENV, IMAGE, ROOT, external_minio, git, ready, run, stop, missing, present


def http(url, token=None, headers=None):
    headers = dict(headers or {})
    if token is not None:
        headers['Authorization'] = 'Basic ' + base64.b64encode(('git:' + token).encode()).decode()
    try:
        response = urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        result = response.status, {key.lower(): value for key, value in response.headers.items()}, response.read()
    return result


def fixture(endpoint, bucket, access_key, secret_key, name):
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        url = f'http://127.0.0.1:{port}/repo'
        writer, reader = secrets.token_hex(32), secrets.token_hex(32)
        token_file = root/'token'; token_file.write_text(writer)
        helper = root/'credential-helper'
        helper.write_text('#!/usr/bin/env python3\nimport sys,pathlib\nfields=dict(line.rstrip("\\n").split("=",1) for line in sys.stdin if "=" in line)\nif sys.argv[1]=="get" and fields.get("path", "repo").startswith("repo"):\n print("username=git\\npassword="+pathlib.Path('+repr(str(token_file))+').read_text().strip())\n')
        helper.chmod(0o700)
        ENV.update(GIT_TERMINAL_PROMPT='0', GIT_CONFIG_COUNT='4',
                   GIT_CONFIG_KEY_0='credential.helper',GIT_CONFIG_VALUE_0=str(helper),
                   GIT_CONFIG_KEY_1='http.proactiveAuth',GIT_CONFIG_VALUE_1='basic',
                   GIT_CONFIG_KEY_2='fetch.uriprotocols',GIT_CONFIG_VALUE_2='http',
                   GIT_CONFIG_KEY_3='credential.useHttpPath',GIT_CONFIG_VALUE_3='true')
        prefix='partial-fixture-uri-'+uuid.uuid4().hex
        variables=dict(endpoint=endpoint,bucket=bucket,access_key=access_key,secret_key=secret_key,prefix=prefix,
                       object_format=name,auth_mode='basic',auth_read_token=reader,auth_write_token=writer,packfile_uri_base=url)
        config=root/'config.toml'
        log_path=root/('uri-'+name+'.log')
        with log_path.open('w') as log:
            def start():
                config.write_text(''.join(f'{key} = {json.dumps(value)}\n' for key,value in variables.items()))
                host=subprocess.Popen(['spin','up','--from',str(ROOT/'spin.toml'),'--listen',f'127.0.0.1:{port}','--variable','@'+str(config)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                try: ready(f'http://127.0.0.1:{port}/.well-known/spin/health',host)
                except BaseException: stop(host,port);raise
                return host
            host=start()
            try:
                source=root/'source'
                git(root,'init','--quiet','-b','main','--object-format='+name,str(source))
                (source/'blob').write_bytes(b'x'*65536)
                (source/'small').write_text('small')
                git(source,'add','.');git(source,'commit','--quiet','-m','first')
                oid=git(source,'rev-parse','HEAD:blob')
                checksum={'sha1':'65a6a6777e4a8a3e4c3213fc9035542b03598d3a','sha256':'a894426233f065e1789eeedf854ba8ff847a23c01e55b62336bf5080b0010524'}[name]
                uri=f'{url}/packfiles/v1/{name}/{oid}/{checksum}.pack'
                git(source,'tag','-a','v1','-m','tag')
                git(source,'push','--quiet',url,'main','v1')
                token_file.write_text(reader)
                clone=root/'clone'
                git(root,'clone','--quiet',url,str(clone))
                assert (clone/'blob').read_bytes()==b'x'*65536
                assert (clone/'.git/objects/pack'/f'pack-{checksum}.pack').exists(), 'URI pack not downloaded'
                git(clone,'fsck','--strict')
                status, headers, pack=http(uri,reader)
                assert status==200 and pack.startswith(b'PACK')
                assert headers['cache-control']=='private, no-store'
                hash_bytes=20 if name=='sha1' else 32
                assert hashlib.new(name,pack[:-hash_bytes]).hexdigest()==checksum
                assert len(pack)==(119 if name=='sha1' else 131), 'WASI canonical golden mismatch'
                assert http(uri)[0]==401 and http(uri,secrets.token_hex(32))[0]==401
                assert http(uri,writer)[0]==200
                ranged=http(uri,reader,{'Range':'bytes=10-'})
                assert ranged[0]==206 and ranged[2]==pack[10:]
                assert ranged[1]['content-range']==f'bytes 10-{len(pack)-1}/{len(pack)}'
                assert http(uri,reader,{'Range':'items=0-1'})[2]==pack
                assert http(uri,reader,{'Range':'bytes=0-4294967296'})[2]==pack
                assert http(uri,reader,{'Range':f'bytes={len(pack)}-'})[0]==416
                assert http(uri,reader,{'Range':'bytes=1-2,4-5'})[0]==400
                assert http(uri,reader,{'Range':'bytes=10-','If-Range':'"wrong"'})[0]==200
                assert http(uri.replace(checksum,'1'*len(checksum)),reader)[0]==400
                # A real retained Git temp pack triggers Range and index-pack verifies it.
                resume=root/'resume';git(root,'init','--quiet','--object-format='+name,str(resume))
                (resume/'.git/objects/pack'/f'pack-{checksum}.pack.temp').write_bytes(pack[:10])
                git(resume,'http-fetch','--packfile='+checksum,'--index-pack-arg=index-pack','--index-pack-arg=--stdin','--index-pack-arg=--keep',uri)
                assert (resume/'.git/objects/pack'/f'pack-{checksum}.pack').read_bytes()==pack
                # No proactive auth: smart auth works but unchanged URI downloader fails.
                failed=subprocess.run(['git','-c','http.proactiveAuth=none','clone','--quiet','--no-checkout',url,str(root/'no-proactive')],env=ENV,capture_output=True,timeout=30)
                assert failed.returncode!=0 and b'401' in failed.stderr, (failed.returncode, failed.stderr.decode())
                filtered=root/'filtered'
                git(root,'clone','--quiet','--no-checkout','--filter=blob:none',url,str(filtered))
                assert oid in missing(filtered)
                shallow=root/'shallow'
                git(root,'clone','--quiet','--depth=1',url,str(shallow))
                # Old URI survives storage maintenance and a completely drained Spin group.
                stop(host,port)
                maintenance_env=dict(ENV,OBJECT_LOG_GIT_MAX_OBJECT_REFS="2080",OBJECT_LOG_MINIO_ENDPOINT=endpoint,OBJECT_LOG_MINIO_ACCESS_KEY=access_key,
                    OBJECT_LOG_MINIO_SECRET_KEY=secret_key,OBJECT_LOG_MINIO_BUCKET=bucket,OBJECT_LOG_PARTIAL_PREFIX=prefix,OBJECT_LOG_PARTIAL_FORMAT=name)
                print(run(['cargo','test','--locked','-p','object-log-git','--features','aws','--test','partial_maintenance','--','--ignored','--nocapture'],ROOT.parent.parent,maintenance_env),flush=True)
                variables['read_only']='true'
                host=start()
                assert http(uri,reader)[2]==pack
                assert git(filtered,'show','HEAD:blob')=='x'*65536
                assert present(filtered,oid)
                git(shallow,'fetch','--quiet','--unshallow');git(shallow,'fsck','--strict')
                # Rotate the read token; old URI still requires the current credential.
                stop(host,port)
                previous=reader;reader=secrets.token_hex(32);variables['auth_read_token']=reader
                variables['read_only']='false';host=start();token_file.write_text(reader)
                assert http(uri,previous)[0]==401 and http(uri,reader)[2]==pack
                # Incremental content is omitted by filter, then fetched lazily through a URI.
                token_file.write_text(writer)
                (source/'blob').write_bytes(b'y'*65536);git(source,'commit','--quiet','-am','next');git(source,'push','--quiet',url,'main')
                token_file.write_text(reader);git(filtered,'fetch','--quiet')
                new_oid=git(source,'rev-parse','HEAD:blob');assert not present(filtered,new_oid)
                assert git(filtered,'show','origin/main:blob')=='y'*65536
                # Removing all refs revokes a previously advertised URI without a lease.
                token_file.write_text(writer);git(source,'push','--quiet',url,':refs/heads/main',':refs/tags/v1')
                assert http(uri,reader)[0]==400
                print(name+': authenticated URI clone, path-scoped helper, native/WASI golden, range resume, filter/lazy/shallow, cold checkpoint/GC, read-only, auth rotation and ref-deletion revocation passed',flush=True)
            except BaseException:
                log.flush()
                print(log_path.read_text(), flush=True)
                raise
            finally: stop(host,port)
        text=log_path.read_text()
        for token in [reader,writer,previous]: assert token not in text


def main():
    external=external_minio();container='object-log-uri-'+uuid.uuid4().hex
    try:
        if external:
            endpoint,bucket,access_key,secret_key=external
            ENV.update(AWS_ACCESS_KEY_ID=access_key,AWS_SECRET_ACCESS_KEY=secret_key)
        else:
            bucket,access_key,secret_key='object-log-test','objectlog','objectlog-local-test-secret'
            run(['docker','run','--detach','--rm','--name',container,'--publish','127.0.0.1::9000','--env','MINIO_ROOT_USER='+access_key,'--env','MINIO_ROOT_PASSWORD='+secret_key,IMAGE,'server','/data'])
            endpoint='http://'+run(['docker','port',container,'9000/tcp'])
        ready(endpoint+'/minio/health/ready')
        if not external: run(['aws','--endpoint-url',endpoint,'s3api','create-bucket','--bucket',bucket])
        for name in ['sha1','sha256']: fixture(endpoint,bucket,access_key,secret_key,name)
    finally:
        if not external: subprocess.run(['docker','rm','--force',container],capture_output=True,check=False,timeout=20)


if __name__=='__main__': main()
