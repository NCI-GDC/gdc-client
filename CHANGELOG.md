<a name"0.2.15-spr2"></a>
### 0.2.15-spr2 (2015-08-31)


#### Bug Fixes

* **version:** update gdc-client version ([61184227](https://github.com/NCI-GDC/gdc-client/commit/61184227))
* **exceptions:** log HTTP exception messages  ([131146d2](https://github.com/NCI-GDC/gdc-client/commit/131146d2))
  * raise_for_status doesn't return the content of the response when raising an exception.
  * Use version of parcel that fixes this issue.
* **urls:** correctly join urls ([ef04dfa6](https://github.com/NCI-GDC/gdc-client/commit/ef04dfa6))
* **related_files:** missing token ([eeb5812c](https://github.com/NCI-GDC/gdc-client/commit/eeb5812c))
  * pass token to related file download
* **script:** undo script overwrite ([d186ea0b](https://github.com/NCI-GDC/gdc-client/commit/d186ea0b))
* **packaging:** fix ubuntu packaging ([ef53c7bb](https://github.com/NCI-GDC/gdc-client/commit/ef53c7bb), closes [#5](https://github.com/NCI-GDC/gdc-client/issues/5))
* **logging:** remove log duplication ([61c5546e](https://github.com/NCI-GDC/gdc-client/commit/61c5546e), closes [#2](https://github.com/NCI-GDC/gdc-client/issues/2))
  * Removes duplicate registration of logger ('client' registered within parcel library and gdc-client proper)
* **parcel:** update dependency ([5c151822](https://github.com/NCI-GDC/gdc-client/commit/5c151822), closes [#4](https://github.com/NCI-GDC/gdc-client/issues/4))

#### Features

* **permissions:** check write permissions ([773b8d9a](https://github.com/NCI-GDC/gdc-client/commit/773b8d9a))

#### Docs

* **contributing:** add contributing reference ([13fe17f7](https://github.com/NCI-GDC/gdc-client/commit/13fe17f7))
* **changelog:** add changelog
