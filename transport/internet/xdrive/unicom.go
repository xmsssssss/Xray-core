package xdrive

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/wopan-sdk-go"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
	"resty.dev/v3"
)

type unicomStorage struct {
	folder       string
	refreshToken string
	accessToken  string
	client       *wopan.WoClient
	httpClient   *http.Client

	idMu sync.Mutex
	ids  map[string]string // flatName -> fid
}

func newUnicomStorage(streamSettings *internet.MemoryStreamConfig, config *Config) (*unicomStorage, error) {
	if config.RemoteFolder == "" {
		return nil, errors.New(`empty "remoteFolder", it must be a WoPan directory id`)
	}
	if len(config.Secrets) < 1 || config.Secrets[0] == "" {
		return nil, errors.New("unicom storage requires at least refreshToken in secrets[0]")
	}

	u := &unicomStorage{
		folder:       config.RemoteFolder,
		refreshToken: config.Secrets[0],
		ids:          make(map[string]string),
		httpClient:   newServiceClient(streamSettings, 60*time.Second, 32),
	}
	if len(config.Secrets) > 1 {
		u.accessToken = config.Secrets[1]
	}

	u.client = wopan.DefaultWithRefreshToken(u.refreshToken)
	if u.accessToken != "" {
		u.client.SetAccessToken(u.accessToken)
	}

	u.client.OnRefreshToken(func(accessToken, refreshToken string) {
		u.accessToken = accessToken
		u.refreshToken = refreshToken
	})

	if err := u.client.InitData(); err != nil {
		return nil, errors.New("failed to init unicom client data").Base(err)
	}

	return u, nil
}

func (u *unicomStorage) rememberID(flat, id string) {
	u.idMu.Lock()
	u.ids[flat] = id
	u.idMu.Unlock()
}

func (u *unicomStorage) forgetID(flat string) {
	u.idMu.Lock()
	delete(u.ids, flat)
	u.idMu.Unlock()
}

func (u *unicomStorage) resolveID(ctx context.Context, flat string) (string, error) {
	u.idMu.Lock()
	id, ok := u.ids[flat]
	u.idMu.Unlock()
	if ok {
		return id, nil
	}

	// 尝试通过 List 刷新缓存
	if _, err := u.List(ctx, ""); err != nil {
		return "", err
	}

	u.idMu.Lock()
	id, ok = u.ids[flat]
	u.idMu.Unlock()
	if ok {
		return id, nil
	}

	return "", errNotFound
}

func (u *unicomStorage) Put(ctx context.Context, name string, data []byte) error {
	flat := flatten(name)

	// 创建临时文件供 SDK 上传
	tmpFile, err := os.CreateTemp("", "xdrive_wo_*")
	if err != nil {
		return errors.New("failed to create temp file for unicom upload").Base(err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	if _, err := tmpFile.Write(data); err != nil {
		return errors.New("failed to write data to temp file").Base(err)
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return errors.New("failed to seek temp file").Base(err)
	}

	uploadFile := wopan.Upload2CFile{
		Name:        flat,
		Size:        int64(len(data)),
		Content:     tmpFile,
		ContentType: "application/octet-stream",
	}

	fid, err := u.client.Upload2CPersonal(uploadFile, u.folder, wopan.Upload2COption{
		Ctx:        ctx,
		RetryTimes: 2,
	})
	if err != nil {
		return errors.New("unicom upload failed for ", name).Base(err)
	}

	if fid != "" {
		u.rememberID(flat, fid)
	}
	return nil
}

func (u *unicomStorage) Get(ctx context.Context, name string) ([]byte, error) {
	flat := flatten(name)
	fid, err := u.resolveID(ctx, flat)
	if err != nil {
		return nil, err
	}

	res, err := u.client.GetDownloadUrlV2([]string{fid}, func(req *resty.Request) {
		req.SetContext(ctx)
	})
	if err != nil {
		return nil, errors.New("unicom GetDownloadUrlV2 failed for ", name).Base(err)
	}
	if len(res.List) == 0 || res.List[0].DownloadUrl == "" {
		u.forgetID(flat)
		return nil, errNotFound
	}

	downloadURL := res.List[0].DownloadUrl

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, errors.New("failed to build download request").Base(err)
	}
	req.Header.Set("User-Agent", wopan.DefaultUA)

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return nil, errors.New("unicom download failed for ", name).Base(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		u.forgetID(flat)
		return nil, errNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("unicom download answered status ", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("failed to read unicom download body").Base(err)
	}
	return body, nil
}

func (u *unicomStorage) deleteFID(ctx context.Context, flat, fid string) error {
	_ = u.client.DeleteFile(wopan.SpaceTypePersonal, nil, []string{fid}, func(req *resty.Request) {
		req.SetContext(ctx)
	})
	u.forgetID(flat)
	return nil
}

func (u *unicomStorage) Delete(ctx context.Context, name string) error {
	flat := flatten(name)

	if fid, err := u.resolveID(ctx, flat); err == nil {
		if err := u.deleteFID(ctx, flat, fid); err != nil {
			return err
		}
	} else if err != errNotFound {
		return err
	}

	// 遍历查找前缀匹配的子文件并删除
	u.idMu.Lock()
	var targets []struct{ flat, fid string }
	prefix := flat + flatSeparator
	for k, v := range u.ids {
		if strings.HasPrefix(k, prefix) {
			targets = append(targets, struct{ flat, fid string }{k, v})
		}
	}
	u.idMu.Unlock()

	for _, t := range targets {
		if err := u.deleteFID(ctx, t.flat, t.fid); err != nil {
			return err
		}
	}
	return nil
}

func (u *unicomStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	flat := ""
	if prefix != "" {
		flat = flatten(prefix) + flatSeparator
	}

	var entries []Entry
	seen := make(map[string]bool)
	pageNum := 0
	pageSize := 100

	for {
		data, err := u.client.QueryAllFilesPersonal(u.folder, pageNum, pageSize, wopan.SortTimeAsc, func(req *resty.Request) {
			req.SetContext(ctx)
		})
		if err != nil {
			return nil, errors.New("unicom QueryAllFiles failed").Base(err)
		}

		for _, f := range data.Files {
			// type 1 为文件
			if f.Type == 1 {
				u.rememberID(f.Name, f.Fid)

				if flat != "" && !strings.HasPrefix(f.Name, flat) {
					continue
				}
				rest := strings.TrimPrefix(f.Name, flat)
				if rest == "" {
					continue
				}
				if cut := strings.Index(rest, flatSeparator); cut >= 0 {
					rest = rest[:cut]
				}
				if seen[rest] {
					continue
				}
				seen[rest] = true
				entries = append(entries, Entry{Name: rest})
			}
		}

		if len(data.Files) < pageSize {
			break
		}
		pageNum++
	}

	return entries, nil
}

func (u *unicomStorage) Close() error {
	return nil
}
