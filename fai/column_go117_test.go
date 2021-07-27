// Copyright ©2021 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// TODO(kortschak): Remove this when go1.16 is no longer supported.

// +build go1.17

package fai

// dropCSVColumn is a no-op.
func dropCSVColumn(err error) error {
	return err
}
