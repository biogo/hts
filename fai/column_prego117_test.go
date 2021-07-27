// Copyright ©2021 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !go1.17

package fai

import "encoding/csv"

// dropCSVColumn drops Column field information. It is required
// to harmonise behaviour between Go versions before and after
// v1.17. The relevant change in the standard library is 6d95e5a4.
func dropCSVColumn(err error) error {
	csvErr, ok := err.(*csv.ParseError)
	if !ok {
		return err
	}
	if csvErr.Err == csv.ErrFieldCount {
		csvErr.Column = 0
	}
	return csvErr
}
