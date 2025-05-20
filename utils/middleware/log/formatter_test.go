package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitize(t *testing.T) {
	t.Run("WhenStringContainsTheWordPassword", func(t *testing.T) {
		expectedOutput := `password`
		val := Sanitize(`password`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutJSONWithPasswordField", func(t *testing.T) {
		expectedOutput := fmt.Sprintf(`{"snapshotsToKeep":0,"password":"%s","daysOfMonth":"1","hour":0,"minute":0}`, PasswordMask)
		val := Sanitize(`{"snapshotsToKeep":0,"password":"secret","daysOfMonth":"1","hour":0,"minute":0}`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutJSONWithUserPasswordField", func(t *testing.T) {
		expectedOutput := fmt.Sprintf(`{"snapshotsToKeep":0,"userPassword":"%s","daysOfMonth":"1","hour":0,"minute":0}`, PasswordMask)
		val := Sanitize(`{"snapshotsToKeep":0,"userPassword":"secret","daysOfMonth":"1","hour":0,"minute":0}`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutXMLWithPasswordAsElementValue", func(t *testing.T) {
		expectedOutput := `<some-operation><desired-attributes>password</desired-attributes></some-operation>`
		val := Sanitize(`<some-operation><desired-attributes>password</desired-attributes></some-operation>`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutXMLWithPasswordStructElement", func(t *testing.T) {
		expectedOutput := `<some-other-operation><password><min-length>12</min-length><max-length>255</max-length></password></some-other-operation>`
		val := Sanitize(`<some-other-operation><password><min-length>12</min-length><max-length>255</max-length></password></some-other-operation>`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutXMLWithPasswordRulesStructElement", func(t *testing.T) {
		expectedOutput := `<some-other-operation><password-rules><min-length>12</min-length><max-length>255</max-length></password-rules></some-other-operation>`
		val := Sanitize(`<some-other-operation><password-rules><min-length>12</min-length><max-length>255</max-length></password-rules></some-other-operation>`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutXMLWithRulesForPasswordStructElement", func(t *testing.T) {
		expectedOutput := `<some-other-operation><rules-for-password><min-length>12</min-length><max-length>255</max-length></rules-for-password></some-other-operation>`
		val := Sanitize(`<some-other-operation><rules-for-password><min-length>12</min-length><max-length>255</max-length></rules-for-password></some-other-operation>`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutADJsonWithUserPasswordField", func(t *testing.T) {
		expectedOutput := fmt.Sprintf(`{"organizationalUnit":"OU=Active,OU=_Servers,DC=rk,DC=com","password":"%s","primaryAD":true}`, PasswordMask)
		val := Sanitize(`{"organizationalUnit":"OU=Active,OU=_Servers,DC=rk,DC=com","password":"\u00267dr-CHOUDHARY{tRP64JyxS%K!","primaryAD":true}`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutApiSecretKeyAndAuthorization", func(t *testing.T) {
		logTemplate := "Content-Type: application/json\r\nApi-Key: %s\r\nSecret-Key: %s\r\nSome-Other-Field: afdabdfsaf\r\nAuthorization: %s\r\nYet-Another-Field: dsafdasf\r\n"
		logOutput := fmt.Sprintf(logTemplate, "apikey", "secretkey", "token dsafasdfsdafdsa")
		expectedOutput := fmt.Sprintf(logTemplate, PasswordMask, PasswordMask, PasswordMask)
		val := Sanitize(logOutput)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutJSONWithPassphraseField", func(t *testing.T) {
		expectedOutput := fmt.Sprintf(`{"replications":[{"clusterLocation":"my-location","created":"2025-01-16T13:34:31.373Z","description":"happyPath","destination":{"volumeId":"2fbcf68d-42d1-4d70-65f0-d2dfdc25ac48","volumeName":"projects/451164690828/locations/day-s1/volumes/happy-path-migrate2"},"healthy":true,"hybridPeeringDetails":{"command":"cluster peer create -peer-addrs 10.9.2.12 -initial-allowed-vserver-peers svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","commandExpiryTime":"2025-01-16T14:34:33.000Z","passphrase":"%s","subnetIp":"10.9.2.0/24"},"hybridReplicationType":"MIGRATION","labels":{"some-key":"some-value","some-key2":"some-value2"},"mirrorState":"PREPARING","replicationSchedule":"HOURLY","resourceId":"repl-happypath2","role":"DESTINATION","state":"PENDING_CLUSTER_PEERING","stateDetails":"Waiting for cluster peering to be created on source cluster"}]}`, PasswordMask)
		val := Sanitize(`{"replications":[{"clusterLocation":"my-location","created":"2025-01-16T13:34:31.373Z","description":"happyPath","destination":{"volumeId":"2fbcf68d-42d1-4d70-65f0-d2dfdc25ac48","volumeName":"projects/451164690828/locations/day-s1/volumes/happy-path-migrate2"},"healthy":true,"hybridPeeringDetails":{"command":"cluster peer create -peer-addrs 10.9.2.12 -initial-allowed-vserver-peers svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","commandExpiryTime":"2025-01-16T14:34:33.000Z","passphrase":"Y3D8+tOltfHbRkfDnNOnl2Jy","subnetIp":"10.9.2.0/24"},"hybridReplicationType":"MIGRATION","labels":{"some-key":"some-value","some-key2":"some-value2"},"mirrorState":"PREPARING","replicationSchedule":"HOURLY","resourceId":"repl-happypath2","role":"DESTINATION","state":"PENDING_CLUSTER_PEERING","stateDetails":"Waiting for cluster peering to be created on source cluster"}]}`)

		assert.Equal(t, expectedOutput, val)
	})
	t.Run("WhenLoggingOutJSONWithPeerIpAddressesField", func(t *testing.T) {
		t.Run("WhenPeerIpAddressesAreSingle", func(ttt *testing.T) {
			expectedOutput := fmt.Sprintf(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerIpAddresses":["%s"]}`, IpMask)
			val := Sanitize(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerIpAddresses": [ "10.23.49.5"  ,   "10.23.49.6"]}`)

			assert.Equal(ttt, expectedOutput, val)
		})
		t.Run("WhenPeerIpAddressesAreMultiple", func(t *testing.T) {
			expectedOutput := fmt.Sprintf(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerIpAddresses":["%s"]}`, IpMask)
			val := Sanitize(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerIpAddresses":["10.23.49.5"]}`)

			assert.Equal(t, expectedOutput, val)
		})
	})
	t.Run("WhenLoggingOutJSONWithPeerAddressesField", func(t *testing.T) {
		t.Run("WhenPeerIpAddressesAreSingle", func(ttt *testing.T) {
			expectedOutput := fmt.Sprintf(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerAddresses":["%s"]}`, IpMask)
			val := Sanitize(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerAddresses": [ "10.23.49.5"  ,   "10.23.49.6"]}`)
			assert.Equal(ttt, expectedOutput, val)
		})
		t.Run("WhenPeerIpAddressesAreMultiple", func(ttt *testing.T) {
			expectedOutput := fmt.Sprintf(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerAddresses":["%s"]}`, IpMask)
			val := Sanitize(`{"peerClusterName": "nkdev-aks-lovely","peerSvmName": "svm_48c1e5981bfe428b9c7cf8f1a4e34a1c_2a3762f0","peerVolumeName": "vol_lovely_s1_basic_volume_2_e827f5","peerAddresses":["10.23.49.5"]}`)

			assert.Equal(ttt, expectedOutput, val)
		})
	})
}
